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
