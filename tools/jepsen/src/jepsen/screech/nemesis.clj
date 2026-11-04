(ns jepsen.screech.nemesis
  (:require [jepsen.control :as c]
            [jepsen.db :as db]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as n]
            [jepsen.net :as net]
            [jepsen.screech.db :as sd]))

(defn heal! [test]
  (when (= "independent-vms" (:topology test)) (sd/action test "heal" nil {}))
  (net/heal! (:net test) test)
  (doseq [node (:nodes test)]
    (db/resume! (:db test) test node)
    (c/with-node test node (db/start! (:db test) test node)))
  :healed)

(defrecord Faults [stats]
  n/Nemesis
  (setup! [this _] this)
  (invoke! [_ test op]
    (let [database (:db test)
          nodes (or (:value op) (:nodes test))
          value (case (:f op)
                  :heal (heal! test)
                  :power-cut (sd/action test "power-cut" nil {:nodes (vec nodes)})
                  (:active-publish :active-ack)
                  (let [[mode operation] (case (:scenario test)
                                           :vm-active-write ["hold" "write"]
                                           :vm-active-flush ["hold" "flush"]
                                           :vm-torn ["tear" "write"]
                                           :vm-eio-write ["eio" "write"]
                                           :vm-eio-flush ["eio" "flush"])]
                    (sd/action test "active-fault" nil {:mode mode :operation operation
                                                        :kind (if (= :active-ack (:f op)) "ack" "publish")}))
                  (:power-bitflip :power-truncate :power-wipe :power-blank)
                  (sd/action test "power-damage" nil {:kind (subs (name (:f op)) 6)})
                  :verify (sd/action test "verify" nil {:nodes (:nodes test)})
                  :partition (net/drop-all! test (n/complete-grudge [(take 2 (:nodes test)) (drop 2 (:nodes test))]))
                  (:kill :start :pause :resume)
                  (into {} (for [node nodes]
                             [node (c/with-node test node
                                     ((case (:f op) :kill db/kill! :start db/start! :pause db/pause! :resume db/resume!) database test node))]))
                  (:wipe :bitflip :truncate)
                  ;; Always damage only n1: never destroy a majority's history.
                  (let [node (first (:nodes test))]
                    (c/with-node test node
                      (db/kill! database test node)
                      (let [result (sd/action test (name (:f op)) node {})]
                        (db/start! database test node)
                        {node result}))))]
      (swap! stats update (:f op) (fnil inc 0))
      (assoc op :value value)))
  (teardown! [_ test] (heal! test))
  n/Reflection
  (fs [_] #{:kill :start :pause :resume :heal :partition :wipe :bitflip :truncate :verify :power-cut :power-bitflip :power-truncate :power-wipe :power-blank :active-publish :active-ack}))

(defn op [f nodes] {:type :info :f f :value nodes})
(defn cycle-for [scenario nodes]
  (let [half (quot (count nodes) 2)
        a (vec (take half nodes))
        pivot [(nth nodes half)]
        b (vec (drop (inc half) nodes))]
    (case scenario
      :baseline [(gen/sleep 5)]
      (:vm-active-write :vm-active-flush :vm-torn :vm-eio-write :vm-eio-flush)
      [(gen/sleep 1) (op :active-publish nil) (op :heal nil) (gen/sleep 5)
       (op :active-ack nil) (op :heal nil) (gen/sleep 5)]
      :vm-power [(gen/sleep 3) (op :power-cut (vec (take (inc half) nodes))) (gen/sleep 2) (op :heal nil)
                 (gen/sleep 5) (op :power-cut nodes) (gen/sleep 2) (op :heal nil) (gen/sleep 5)]
      :vm-damage [(gen/sleep 3) (op :power-bitflip nodes) (op :heal nil) (gen/sleep 7)
                  (op :power-truncate nodes) (op :heal nil) (gen/sleep 7)]
      :vm-wipe [(gen/sleep 3) (op :power-wipe nodes) (op :heal nil) (gen/sleep 10)]
      :vm-blank [(gen/sleep 3) (op :power-blank nodes) (op :heal nil) (gen/sleep 10)]
      :power [(gen/sleep 3) (op :kill (take (inc half) nodes)) (gen/sleep 3) (op :heal nil)
              (gen/sleep 3) (op :kill nodes) (gen/sleep 3) (op :heal nil)]
      :pause-kill [(gen/sleep 3) (op :pause a) (gen/sleep 3) (op :kill pivot)
                   (op :resume a) (op :pause b) (op :start pivot) (gen/sleep 10) (op :heal nil)]
      :partition [(gen/sleep 3) (op :partition nil) (gen/sleep 4) (op :kill [(last nodes)])
                  (gen/sleep 2) (op :heal nil)]
      :corrupt [(gen/sleep 3) (op :bitflip nil) (gen/sleep 8) (op :truncate nil) (gen/sleep 8)]
      :wipe [(gen/sleep 3) (op :wipe nil) (gen/sleep 10)])))

(defn generator [scenario nodes]
  (let [schedule (cycle-for scenario nodes)]
    (if (#{:vm-active-write :vm-active-flush :vm-torn :vm-eio-write :vm-eio-flush} scenario)
      (concat schedule (repeat (gen/sleep 5)))
      (cycle schedule))))
