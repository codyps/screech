(ns jepsen.screech.consumer-test
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.screech.consumer :as c]))

(def nodes ["n1" "n2" "n3"])
(def fixture-events
  [{:kind :stream :stream "L_x" :retention "limits"}
   {:kind :consumer :stream "L_x" :consumer "D" :epoch 0 :request 1}
   {:kind :consumer :stream "L_x" :consumer "GONE" :epoch 0 :request 2}
   {:kind :delete :stream "L_x" :consumer "GONE" :epoch 0 :request 3}
   {:kind :publish :stream "L_x" :seq 1 :role :stable :body "stable"}
   {:kind :deliver :stream "L_x" :consumer "D" :epoch 0 :seq 1 :cseq 1 :body "stable"}
   {:kind :attempt :action :ack :stream "L_x" :consumer "D" :epoch 0 :seq 1 :cseq 1 :request 2}
   {:kind :ack :stream "L_x" :consumer "D" :epoch 0 :seq 1 :cseq 1 :confirmed true :request 2}
   {:kind :dedup :stream "L_x" :seq 1 :duplicate true}
   {:kind :stream :stream "I_x" :retention "interest"}
   {:kind :consumer :stream "I_x" :consumer "D" :epoch 0 :request 3}
   {:kind :consumer :stream "I_x" :consumer "E" :epoch 0 :request 4}
   {:kind :publish :stream "I_x" :seq 1 :role :removed :body "interest-removed"}
   {:kind :ack :stream "I_x" :consumer "D" :epoch 0 :seq 1 :cseq 1 :confirmed true}
   {:kind :ack :stream "I_x" :consumer "E" :epoch 0 :seq 1 :cseq 1 :confirmed true}
   {:kind :publish :stream "I_x" :seq 2 :role :pending :body "interest-retained"}
   {:kind :ack :stream "I_x" :consumer "D" :epoch 0 :seq 2 :cseq 2 :confirmed true}
   {:kind :deliver :stream "I_x" :consumer "E" :epoch 0 :seq 2 :cseq 2 :body "interest-retained"}
   {:kind :stream :stream "W_x" :retention "workqueue"}
   {:kind :consumer :stream "W_x" :consumer "D" :epoch 0 :request 5}
   {:kind :publish :stream "W_x" :seq 1 :role :removed :body "work-removed"}
   {:kind :ack :stream "W_x" :consumer "D" :epoch 0 :seq 1 :cseq 1 :confirmed true}
   {:kind :publish :stream "W_x" :seq 2 :role :pending :body "work-retained"}
   {:kind :deliver :stream "W_x" :consumer "D" :epoch 0 :seq 2 :cseq 2 :generation "10" :body "work-retained"}
   {:kind :delete :stream "W_x" :consumer "D" :epoch 0 :request 6}
   {:kind :consumer :stream "W_x" :consumer "D" :epoch 1 :request 7}
   {:kind :deliver :stream "W_x" :consumer "D" :epoch 1 :seq 2 :cseq 1 :generation "11" :body "work-retained"}
   {:kind :stale :stream "W_x" :confirmed false}
   {:kind :complete}])
(def missing {:error {:code 404}})
(defn info [retention last-seq]
  {:config {:retention retention :num_replicas 3} :state {:messages 1 :last_seq last-seq}})
(defn consumer [floor delivered pending]
  {:config {:ack_policy "explicit" :ack_wait 3600000000000}
   :ack_floor {:consumer_seq floor :stream_seq floor}
   :delivered {:consumer_seq delivered} :num_ack_pending pending})
(def snapshot
  {:dedup {:duplicate true :seq 1} :work-dedup {:duplicate true :seq 1} :stale {:confirmed false}
   :streams {"L_x" {:info (info "limits" 1) :consumers {"D" (consumer 1 1 0) "GONE" missing}
                     :messages {1 {:seq 1 :body "stable"}}}
             "I_x" {:info (info "interest" 2) :consumers {"D" (consumer 2 2 0) "E" (consumer 1 2 1)}
                     :messages {1 missing 2 {:seq 2 :body "interest-retained"}}}
             "W_x" {:info (info "workqueue" 2) :consumers {"D" (consumer 0 1 1)}
                     :messages {1 missing 2 {:seq 2 :body "work-retained"}}}}})
(defn check-history [events snapshots]
  (checker/check (c/->Checker) {:nodes nodes}
                 (concat [{:f :exercise :type :info :value {:id "x" :seed true :events events}}]
                         (for [[node snapshot] snapshots]
                           {:f :audit :type :ok :value {:node node :cases {"x" snapshot}}})) {}))
(defn check-snapshot [s] (check-history fixture-events (zipmap nodes (repeat s))))
(defn errors [s] (set (map :kind (:errors (check-snapshot s)))))

(deftest healthy-history-and-damaged-observations
  ;; The enclosing cycle can time out after its confirmed steps. Those results
  ;; remain binding: treating all :info cycles as unknown would miss these bugs.
  (is (true? (:valid? (check-snapshot snapshot))))
  (is (= 5 (:confirmed-acks (check-snapshot snapshot))))
  (doseq [[path value expected]
          [[[:streams "L_x" :consumers "D" :ack_floor :consumer_seq] 0 :confirmed-ack-rollback]
           [[:streams "L_x" :consumers "D" :ack_floor :stream_seq] 0 :confirmed-stream-ack-rollback]
           [[:streams "I_x" :consumers "E" :ack_floor :consumer_seq] 2 :ack-floor-passed-pending-delivery]
           [[:streams "I_x" :consumers "E" :ack_floor :stream_seq] 2 :stream-ack-floor-passed-pending-message]
           [[:streams "W_x" :messages 1] {:body "work-removed"} :retention-resurrected]
           [[:streams "I_x" :messages 1] {:body "interest-removed"} :retention-resurrected]
           [[:streams "I_x" :messages 2] missing :lost-or-changed-payload]
           [[:streams "L_x" :messages 1] missing :lost-or-changed-payload]
           [[:streams "L_x" :messages 1 :body] "retry-bytes" :lost-or-changed-payload]
           [[:work-dedup :seq] 3 :dedup-after-retention-rollback]
           [[:streams "L_x" :consumers "GONE"] (consumer 0 0 0) :resurrected-consumer]
           [[:dedup :seq] 2 :dedup-rollback]
           [[:dedup :duplicate] false :dedup-rollback]
           [[:streams "L_x" :consumers "D" :config :ack_policy] "none" :ack-policy-reset]
           [[:streams "L_x" :consumers "D" :config :ack_wait] 1 :ack-wait-reset]
           [[:streams "L_x" :consumers "D" :num_ack_pending] 1 :wrong-pending-count]
           [[:streams "L_x" :info :state :messages] 2 :duplicate-created-record]
           [[:streams "L_x" :info :state :last_seq] 2 :duplicate-advanced-sequence]
           [[:streams "W_x" :consumers "D" :ack_floor :consumer_seq] 1 :stale-ack-advanced-floor]
           [[:streams "W_x" :consumers "D" :num_ack_pending] 0 :stale-ack-removed-reservation]
           [[:streams "I_x" :consumers "E" :num_ack_pending] 0 :lost-reservation]
           [[:streams "I_x" :consumers "E" :delivered :consumer_seq] 1 :delivery-reservation-rollback]
           [[:streams "W_x" :consumers "D"] missing :lost-consumer]
           [[:streams "I_x" :info] missing :lost-stream]
           [[:stale :confirmed] true :stale-ack-confirmed-after-recovery]]]
    (is (contains? (errors (assoc-in snapshot path value)) expected) (str expected)))
  (is (false? (:valid? (check-history fixture-events {"n1" snapshot "n2" snapshot}))))
  (is (false? (:valid? (check-history fixture-events {"n1" snapshot "n2" snapshot "n3" (assoc-in snapshot [:streams "L_x" :messages 1] missing)}))))
  (is (false? (:valid? (check-history [] (zipmap nodes (repeat snapshot)))))))

(deftest unknown-ack-is-not-a-confirmation
  (let [events [{:kind :stream :stream "W_y" :retention "workqueue"}
                {:kind :publish :stream "W_y" :seq 1 :role :removed :body "value"}
                {:kind :attempt :action :ack :stream "W_y" :consumer "D" :seq 1}]
        observed {:work-dedup {:duplicate true :seq 1} :streams {"W_y" {:info (info "workqueue" 1) :messages {1 missing}}}}]
    (is (empty? (c/issues events observed 3 false)))
    (is (empty? (c/issues events (assoc-in observed [:streams "W_y" :messages 1] {:body "value"}) 3 false)))
    (is (some #(= :lost-or-changed-payload (:kind %)) (c/issues (pop events) observed 3 false)))))

(deftest completed-steps-survive-later-request-timeouts
  (with-redefs [c/exercise! (fn [_ _ journal _ _]
                            (swap! journal conj {:kind :ack :confirmed true :seq 1})
                            (c/unavailable "reply lost later"))]
    (let [result (client/invoke! (c/->Client nil "n1" (atom {})) {:nodes nodes} {:f :exercise})]
      (is (= :info (:type result)))
      (is (= :ack (get-in result [:value :events 0 :kind])))
      (is (true? (get-in result [:value :events 0 :confirmed])))
      (is (false? (get-in result [:error :violation]))))))

(deftest failed-audits-retain-invalid-probe-results
  (let [healthy [{:f :exercise :type :ok :value {:id "x" :seed true :events fixture-events}}]
        audits (for [node nodes] {:f :audit :type :ok :value {:node node :cases {"x" snapshot}}})]
    (doseq [event [{:kind :dedup :duplicate false :seq 2}
                  {:kind :stale :confirmed true}]]
      (let [history (concat healthy [{:f :audit :type :info :value {:node "n1" :events [event]}}] audits)
            result (checker/check (c/->Checker) {:nodes nodes} history {})]
        (is (false? (:valid? result)))
        (is (some #(= :invalid-probe-result (:kind %)) (:errors result)))))))

(deftest successful-reads-in-failed-audits-still-bind
  (let [seed {:f :exercise :type :ok :value {:id "x" :seed true :events fixture-events}}
        audits (for [node nodes] {:f :audit :type :ok :value {:node node :cases {"x" snapshot}}})
        check-read (fn [event]
                     (checker/check (c/->Checker) {:nodes nodes}
                                    (concat [seed {:f :audit :type :info
                                                   :value {:node "n1" :events [(merge {:kind :read :case "x"} event)]}}]
                                            audits) {}))]
    (is (true? (:valid? (check-read {:stream "I_x" :path [:messages 1] :result missing}))))
    (doseq [event [{:stream "L_x" :path [:consumers "D"] :result (consumer 0 1 0)}
                  {:stream "W_x" :path [:messages 1] :result {:body "work-removed"}}
                  {:stream "I_x" :path [:messages 2] :result missing}
                  {:stream "L_x" :path [:consumers "GONE"] :result (consumer 0 0 0)}]]
      (is (false? (:valid? (check-read event)))))))

(deftest audit-does-not-start-another-mutation-cycle
  (with-redefs [c/exercise! (fn [& _] (throw (ex-info "audit dispatched as exercise" {:violation true})))
                c/audit! (fn [& _] {"saved-case" snapshot})]
    (let [catalog (atom {})
          result (client/invoke! (c/->Client nil "n1" catalog) {:nodes nodes} {:f :audit})]
      (is (= :ok (:type result)))
      (is (= snapshot (get-in result [:value :cases "saved-case"])))
      (is (empty? @catalog)))))
