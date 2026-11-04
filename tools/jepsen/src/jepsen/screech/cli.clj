(ns jepsen.screech.cli
  (:gen-class)
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.core :as core]
            [jepsen.generator :as gen]
            [jepsen.os :as os]
            [jepsen.tests :as tests]
            [jepsen.screech.db :as db]
            [jepsen.screech.consumer :as consumer]
            [jepsen.screech.nemesis :as nemesis]
            [jepsen.screech.queue :as queue]
            [jepsen.screech.remote :as remote]))

(def required-faults
  {:baseline {} :power {:kill 2} :pause-kill {:pause 2 :kill 1 :resume 1 :start 1}
   :partition {:partition 1 :kill 1} :corrupt {:bitflip 1 :truncate 1} :wipe {:wipe 1}
   :vm-power {:power-cut 2} :vm-damage {:power-bitflip 1 :power-truncate 1}
   :vm-wipe {:power-wipe 1} :vm-blank {:power-blank 1}})

(def active-scenarios
  {:vm-active-write ["hold" "write"] :vm-active-flush ["hold" "flush"]
   :vm-torn ["tear" "write"] :vm-eio-write ["eio" "write"] :vm-eio-flush ["eio" "flush"]})

(defn active-proof? [nodes proof scenario]
  (if-let [[mode operation] (active-scenarios scenario)]
    (let [faults (:io_faults proof)
          kinds (set (map :request_kind faults))]
      (boolean
        (and (= #{"publish" "ack"} kinds)
             (every? (fn [fault]
                       (let [targets (set (:targets fault))
                             survivors (set (:surviving_nodes fault))
                             cut (get (:power_events proof) (:cut_index fault))
                             requests (:requests fault)]
                         (and (= mode (:mode fault)) (= operation (:operation fault))
                              (contains? targets (:leader fault))
                              (seq targets) (<= (count targets) (quot (count nodes) 2))
                              (= (set nodes) (into targets survivors))
                              (not-any? targets survivors)
                              (>= (count survivors) (inc (quot (count nodes) 2)))
                              (= targets (set (:nodes cut)))
                              (false? (:voter_threads_paused cut))
                              (= "host-SIGSTOP-without-I/O-drain" (:pause_mechanism cut))
                              (seq requests)
                              (every? #(and (string? (:id %)) (seq (:id %))
                                            (= (:request_kind fault) (:kind %))) requests)
                              (if (= mode "eio")
                                (and (= 5 (:errno fault)) (true? (:reply_sent fault)) (seq (:guest_errors fault)))
                                (some (set (map :id requests)) (map :id (:requests_at_cut fault))))
                              (or (not= mode "tear")
                                  (and (>= (:length fault 0) 1024) (= 512 (:persisted_bytes fault))
                                       (seq (:extents fault))
                                       (some #(and (<= (:physical %) (:offset fault))
                                                   (<= (+ (:offset fault) (:length fault))
                                                       (+ (:physical %) (:length %)))) (:extents fault))
                                       (= (:cold_sha256 fault) (:expected_sha256 fault) (:after_sha256 fault))
                                       (string? (:cold_sha256 fault)) (= 64 (count (:cold_sha256 fault)))
                                       (not= (:cold_sha256 fault) (:before_sha256 fault))
                                       (not= (:cold_sha256 fault) (:requested_sha256 fault))))))) faults))))
    true))

(defn active-history? [nodes proof scenario history]
  (or (not (active-scenarios scenario))
      (let [attempts (into {} (keep #(when (:io-id %) [(:io-id %) (name (:action %))])
                                   (consumer/completed-events history)))
            recovered (set (keep #(when (and (= :exercise (:f %)) (= :ok (:type %))
                                              (get-in % [:value :recovery]))
                                     (get-in % [:value :node])) history))]
        (and (= (set nodes) recovered)
             (every? (fn [fault]
                       (every? #(= (:request_kind fault) (attempts (:id %))) (:requests fault)))
                     (:io_faults proof))))))

(defn valid-power-control? [control]
  (and (true? (:fsynced_preserved control)) (true? (:unsynced_lost control))
       (string? (:old_boot_id control)) (seq (:old_boot_id control))
       (string? (:new_boot_id control)) (seq (:new_boot_id control))
       (not= (:old_boot_id control) (:new_boot_id control))))

(defn independent-proof? [nodes proof]
  (let [ids (:boot_ids proof)
        controls (:power_canaries proof)
        cuts (:power_events proof)]
    (boolean
      (and (:independent_vms proof) (:converged proof)
           (= (set nodes) (set (map name (keys ids))))
           (every? #(and (string? %) (seq %)) (vals ids))
           (= (count nodes) (count (set (vals ids))))
           (= (set nodes) (set (map name (keys controls))))
           (every? #(and (:verified %) (>= (:boots % 0) 2) (seq (:checks %))
                         (every? valid-power-control? (:checks %))) (vals controls))
           (seq cuts)
           (every? (fn [cut]
                     (and (= "SIGKILL" (:signal cut)) (:all_paused_before_kill cut)
                          (<= 0 (:dispatch_skew_ms cut 1000) 250)
                          (= (set (:nodes cut)) (set (map name (keys (:verified cut)))))
                          (= (set (:nodes cut)) (set (map name (keys (:old_boot_ids cut)))))
                          (= (count (:nodes cut)) (count (set (vals (:old_boot_ids cut)))))
                          (= (count (:nodes cut)) (count (set (map :new_boot_id (vals (:verified cut))))))
                          (every? (fn [[node c]] (and (valid-power-control? c)
                                                      (= (:old_boot_id c) (get (:old_boot_ids cut) node))))
                                  (:verified cut)))) cuts)))))

(defn assurance [database fault-stats scenario]
  (reify checker/Checker
    (check [_ test history _]
      (let [acknowledged (if (= :consumer-lifecycle (:workload test))
                           (consumer/acknowledged history)
                           (count (filter #(and (= :publish (:f %)) (= :ok (:type %))) history)))
            native? (= "independent-vms" (:topology test))
            proof (last (keep #(when (and (= :verify (:f %)) (get-in % [:value :converged])) (:value %)) history))
            canaries (if native?
                       (and (= (count (:nodes test)) (:vm-nodes @(:stats database)))
                            (independent-proof? (:nodes test) proof)
                            (active-proof? (:nodes test) proof scenario))
                       (= (count (:nodes test)) (:canaries @(:stats database))))
            required (some-> (if (active-scenarios scenario)
                               {:active-publish 1 :active-ack 1}
                               (required-faults scenario)) (assoc :verify 1))
            required (or required (throw (ex-info "Unknown fault scenario" {:scenario scenario})))
            faults (every? (fn [[f minimum]] (<= minimum (get @fault-stats f 0))) required)
            recovered (active-history? (:nodes test) proof scenario history)
            clears (or native? (= scenario :baseline) (pos? (get @(:stats database) :cache-clears 0)))]
        {:valid? (boolean (and canaries faults recovered clears (pos? acknowledged)))
         :acknowledged acknowledged :lazyfs (when-not native? @(:stats database))
         :native-ext4 (when native? @(:stats database)) :faults @fault-stats
         :required-faults required
         :active-io (when (active-scenarios scenario) {:verified (active-proof? (:nodes test) proof scenario)
                                                     :recovery-cycles-completed recovered
                                                     :faults (count (:io_faults proof))})
         :independent-vms (when native? {:verified (independent-proof? (:nodes test) proof)
                                         :boot-ids (:boot_ids proof) :power-cuts (count (:power_events proof))
                                         :disk-damage (count (:disk_damage proof))})}))))

(defn test-map [opts]
  (let [nodes (mapv #(str "n" %) (range 1 (inc (:node-count opts))))
        database (db/database)
        workload-fn (case (:workload opts :queue)
                      :queue queue/workload
                      :consumer-lifecycle consumer/workload)
        workload (workload-fn (assoc opts :nodes nodes))
        fault-stats (atom {})
        generator (gen/phases
                    (when-let [initial (:initial-generator workload)]
                      (->> initial gen/clients (gen/time-limit 120)))
                    (->> (:generator workload)
                         (gen/stagger (/ 1.0 (:rate opts)))
                         (gen/nemesis (nemesis/generator (:scenario opts) nodes))
                         (gen/time-limit (:time-limit opts)))
                    (gen/nemesis [(nemesis/op :heal nil) (gen/sleep 3)])
                    (->> (:final-generator workload) gen/clients (gen/time-limit (:final-time-limit opts)))
                    (gen/nemesis [(nemesis/op :verify nil)]))]
    (merge tests/noop-test opts
           {:name (str (if (= "independent-vms" (:topology opts)) "screech-vm-" "screech-lazyfs-") (name (:workload opts :queue)) "-" (name (:scenario opts)))
            :nodes nodes :os os/noop :remote (remote/remote (:control-endpoint opts)) :db database
            :cluster-id (str (java.util.UUID/randomUUID))
            :cluster-secret (str (java.util.UUID/randomUUID) (java.util.UUID/randomUUID))
            :client (:client workload)
            :nemesis (nemesis/->Faults fault-stats)
            :checker (checker/compose {:stats (checker/stats)
                                       :exceptions (checker/unhandled-exceptions)
                                       :workload (:checker workload)
                                       :assurance (assurance database fault-stats (:scenario opts))})
            :generator ((:wrap-generator workload) generator)})))

(defn -main [& args]
  (let [opts (merge {:node-count 5 :concurrency 10 :rate 30 :time-limit 30 :final-time-limit 60}
                    (json/parse-string (slurp (first args)) true))
        opts (-> opts (update :scenario keyword) (update :workload #(keyword (or % "queue"))))
        result (core/run! (test-map opts))
        summary {:workload (:workload opts) :scenario (:scenario opts) :valid? (get-in result [:results :valid?])
                 :results (:results result)}]
    (spit "summary.json" (json/generate-string summary {:pretty true}))
    (println "SCREECH_JEPSEN_RESULT" (json/generate-string summary))
    (shutdown-agents)
    (System/exit (if (true? (:valid? summary)) 0 1))))
