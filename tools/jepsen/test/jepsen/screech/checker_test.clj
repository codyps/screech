(ns jepsen.screech.checker-test
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [jepsen.history :as h]
            [jepsen.screech.cli :as cli]
            [jepsen.screech.db :as db]
            [jepsen.screech.queue :as queue]))

(def nodes ["n1" "n2" "n3"])
(defn completed [process f value]
  [{:type :invoke :process process :f f :value value}
   {:type :ok :process process :f f :value value}])
(defn history [reads]
  (h/history (concat (completed 0 :publish "0-1")
                     (completed 0 :publish "0-2")
                     (mapcat (fn [[i values]] (completed i :fetch values))
                             (map-indexed vector reads)))))
(defn check-reads [reads]
  ;; Unit tests skip rendering; the real campaign renders all upstream plots.
  (with-redefs [queue/lost-plot! (fn [& _] (doto (promise) (deliver true)))]
    (checker/check (queue/->Checker) {:nodes nodes} (history reads) {})))

(deftest all-nodes-must-retain-every-acknowledged-write
  (let [both ["0-1" "0-2"]]
    (is (true? (:valid? (check-reads [both both both]))))
    (let [result (check-reads [both ["0-2"] both])]
      (is (false? (:valid? result)))
      (is (zero? (get-in result [:lost :lost-count])))
      (is (= 1 (get-in result [:lost-on-node "n2" :lost-count]))))
    (is (= 1 (get-in (check-reads [["0-1"] ["0-1"] ["0-1"]]) [:lost :lost-count])))
    (is (false? (:valid? (check-reads [both both (conj both "99-1")]))))))

(deftest vacuous-or-incomplete-runs-fail
  (let [database (db/database)
        stats (atom {:verify 1})
        test {:nodes nodes}
        ack [{:type :ok :f :publish :value "0-1"}]
        baseline (cli/assurance database stats :baseline)
        power (cli/assurance database stats :power)]
    (is (false? (:valid? (checker/check baseline test ack {}))))
    (reset! (:stats database) {:canaries 3 :cache-clears 3})
    (is (false? (:valid? (checker/check baseline test [] {}))))
    (is (true? (:valid? (checker/check baseline test ack {}))))
    (reset! stats {:verify 1 :kill 1})
    (is (false? (:valid? (checker/check power test ack {}))))
    (reset! stats {:kill 2})
    (is (false? (:valid? (checker/check power test ack {}))))
    (reset! stats {:verify 1 :kill 2})
    (is (true? (:valid? (checker/check power test ack {}))))
    (reset! (:stats database) {:canaries 3})
    (is (false? (:valid? (checker/check power test ack {}))))))

(def power-check {:old_boot_id "old" :new_boot_id "new" :fsynced_preserved true :unsynced_lost true})
(def power-checks
  (into {} (for [node nodes] [(keyword node) (assoc power-check :old_boot_id (str "old-" node) :new_boot_id (str "new-" node))])))
(def vm-proof
  {:independent_vms true :converged true
   :boot_ids {:n1 "kernel-1" :n2 "kernel-2" :n3 "kernel-3"}
   :power_canaries (into {} (for [[node check] power-checks] [node {:verified true :boots 2 :checks [check]}]))
   :power_events [{:nodes nodes :signal "SIGKILL" :all_paused_before_kill true :dispatch_skew_ms 1
                   :old_boot_ids (into {} (map (fn [[node check]] [node (:old_boot_id check)]) power-checks))
                   :verified power-checks}]})

(deftest independent-vm-evidence-must-prove-cold-reboots-and-cache-loss
  (is (true? (cli/independent-proof? nodes vm-proof)))
  (doseq [[path value]
          [[[:boot_ids :n3] "kernel-1"]
           [[:boot_ids :n3] nil]
           [[:power_canaries :n2 :boots] 1]
           [[:power_canaries :n2 :checks] []]
           [[:power_canaries :n2 :checks 0 :fsynced_preserved] false]
           [[:power_canaries :n2 :checks 0 :unsynced_lost] false]
           [[:power_canaries :n2 :checks 0 :new_boot_id] "old-n2"]
           [[:power_events] []]
           [[:power_events 0 :signal] "SIGTERM"]
           [[:power_events 0 :all_paused_before_kill] false]
           [[:power_events 0 :dispatch_skew_ms] 251]
           [[:power_events 0 :verified] {:n1 power-check}]
           [[:power_events 0 :old_boot_ids :n2] "different-kernel"]]]
    (is (false? (cli/independent-proof? nodes (assoc-in vm-proof path value))) (str path))))

(def active-cut
  {:nodes ["n1"] :signal "SIGKILL" :all_paused_before_kill true :voter_threads_paused false
   :pause_mechanism "host-SIGSTOP-without-I/O-drain"
   :dispatch_skew_ms 0 :old_boot_ids {:n1 "old-n1"} :verified (select-keys power-checks [:n1])})
(def active-faults
  (vec (for [[index kind] (map-indexed vector ["publish" "ack"])]
         {:mode "tear" :operation "write" :leader "n1" :targets ["n1"]
          :surviving_nodes ["n2" "n3"] :request_kind kind :cut_index (inc index)
          :membership_before {:voters [1 2 3] :outgoing [] :learners [] :replacement nil}
          :replicas_before {:n1 {:member 1} :n2 {:member 2} :n3 {:member 3}}
          :requests [{:id kind :kind kind}] :requests_at_cut [{:id kind :kind kind}]
          :offset 4096 :length 4096 :persisted_bytes 512 :extents [{:physical 4096 :length 4096}]
          :before_sha256 (apply str (repeat 64 "0")) :requested_sha256 (apply str (repeat 64 "1"))
          :after_sha256 (apply str (repeat 64 "2")) :expected_sha256 (apply str (repeat 64 "2"))
          :cold_sha256 (apply str (repeat 64 "2"))})))
(def active-proof (-> vm-proof (update :power_events into [active-cut active-cut]) (assoc :io_faults active-faults)))

(deftest active-faults-must-interrupt-real-client-and-disk-work-with-a-surviving-quorum
  (is (true? (cli/independent-proof? nodes active-proof)))
  (is (true? (cli/active-proof? nodes active-proof :vm-torn)))
  (doseq [[path value]
          [[[:io_faults] []]
           [[:io_faults 1 :request_kind] "publish"]
           [[:io_faults 0 :requests] []]
           [[:io_faults 0 :requests 0 :kind] "ack"]
           [[:io_faults 0 :requests_at_cut] []]
           [[:io_faults 0 :targets] ["n1" "n2"]]
           [[:io_faults 0 :surviving_nodes] ["n1" "n2"]]
           [[:io_faults 0 :membership_before :learners] [4]]
           [[:io_faults 0 :membership_before :replacement] {:node 1}]
           [[:io_faults 0 :replicas_before :n2 :member] 4]
           [[:io_faults 0 :leader] "n3"]
           [[:io_faults 0 :cut_index] 0]
           [[:power_events 1 :voter_threads_paused] true]
           [[:power_events 1 :pause_mechanism] "QMP-stop"]
           [[:io_faults 0 :persisted_bytes] 4096]
           [[:io_faults 0 :extents] []]
           [[:io_faults 0 :cold_sha256] (apply str (repeat 64 "1"))]]]
    (is (false? (cli/active-proof? nodes (assoc-in active-proof path value) :vm-torn)) (str path)))
  (let [proof (update active-proof :io_faults
                      #(mapv (fn [f] (assoc f :mode "eio" :operation "flush" :errno 5 :reply_sent true
                                            :error_after_canary true :subsequent_io_held true
                                            :guest_errors ["I/O error, dev vda"])) %))]
    (is (true? (cli/active-proof? nodes proof :vm-eio-flush)))
    (is (false? (cli/active-proof? nodes (assoc-in proof [:io_faults 0 :errno] 0) :vm-eio-flush)))
    (is (false? (cli/active-proof? nodes (assoc-in proof [:io_faults 0 :reply_sent] false) :vm-eio-flush)))
    (is (false? (cli/active-proof? nodes (assoc-in proof [:io_faults 0 :guest_errors] []) :vm-eio-flush)))
    (is (false? (cli/active-proof? nodes (assoc-in proof [:io_faults 0 :error_after_canary] false) :vm-eio-flush)))
    (is (false? (cli/active-proof? nodes (assoc-in proof [:io_faults 0 :subsequent_io_held] false) :vm-eio-flush)))))

(deftest active-windows-must-match-history-attempts-and-completed-recovery-cycles
  (let [history (concat
                  [{:type :info :f :exercise
                    :value {:events [{:kind :attempt :action :publish :io-id "publish"}
                                     {:kind :attempt :action :ack :io-id "ack"}]}}]
                  (for [node nodes] {:type :ok :f :exercise :value {:node node :recovery true}}))]
    (is (true? (cli/active-history? nodes active-proof :vm-torn history)))
    (is (false? (cli/active-history? nodes active-proof :vm-torn (rest history))))
    (is (false? (cli/active-history? nodes active-proof :vm-torn (butlast history))))))
