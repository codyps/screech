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
            [jepsen.screech.nemesis :as nemesis]
            [jepsen.screech.queue :as queue]
            [jepsen.screech.remote :as remote]))

(def required-faults
  {:baseline {} :power {:kill 2} :pause-kill {:pause 2 :kill 1 :resume 1 :start 1}
   :partition {:partition 1 :kill 1} :corrupt {:bitflip 1 :truncate 1} :wipe {:wipe 1}})

(defn assurance [database fault-stats scenario]
  (reify checker/Checker
    (check [_ test history _]
      (let [acknowledged (count (filter #(and (= :publish (:f %)) (= :ok (:type %))) history))
            canaries (= (count (:nodes test)) (:canaries @(:stats database)))
            required (some-> (required-faults scenario) (assoc :verify 1))
            required (or required (throw (ex-info "Unknown fault scenario" {:scenario scenario})))
            faults (every? (fn [[f minimum]] (<= minimum (get @fault-stats f 0))) required)
            clears (or (= scenario :baseline) (pos? (get @(:stats database) :cache-clears 0)))]
        {:valid? (boolean (and canaries faults clears (pos? acknowledged)))
         :acknowledged acknowledged :lazyfs @(:stats database) :faults @fault-stats
         :required-faults required}))))

(defn test-map [opts]
  (let [nodes (mapv #(str "n" %) (range 1 (inc (:node-count opts))))
        database (db/database)
        workload (queue/workload (assoc opts :nodes nodes))
        fault-stats (atom {})
        generator (gen/phases
                    (->> (:generator workload)
                         (gen/stagger (/ 1.0 (:rate opts)))
                         (gen/nemesis (nemesis/generator (:scenario opts) nodes))
                         (gen/time-limit (:time-limit opts)))
                    (gen/nemesis [(nemesis/op :heal nil) (gen/sleep 3)])
                    (->> (:final-generator workload) gen/clients (gen/time-limit (:final-time-limit opts)))
                    (gen/nemesis [(nemesis/op :verify nil)]))]
    (merge tests/noop-test opts
           {:name (str "screech-lazyfs-" (name (:scenario opts)))
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
        opts (update opts :scenario keyword)
        result (core/run! (test-map opts))
        summary {:scenario (:scenario opts) :valid? (get-in result [:results :valid?])
                 :results (:results result)}]
    (spit "summary.json" (json/generate-string summary {:pretty true}))
    (println "SCREECH_JEPSEN_RESULT" (json/generate-string summary))
    (shutdown-agents)
    (System/exit (if (true? (:valid? summary)) 0 1))))
