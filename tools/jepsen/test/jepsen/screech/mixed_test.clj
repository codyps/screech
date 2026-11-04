(ns jepsen.screech.mixed-test
  (:require [clojure.test :refer :all]
            [jepsen.screech.mixed :as m]))

(def nodes ["n1" "n2" "n3"])
(defn canary [edges boots epoch]
  {:epoch epoch :blocked (vec edges) :attempted (vec (m/pairs nodes)) :received (vec (remove edges (m/pairs nodes)))
   :boot_ids boots
   :edges (vec (for [[a b :as pair] (m/pairs nodes)]
                  {:source a :destination b :dropped (if (edges pair) 1 0) :forwarded (if (edges pair) 0 1)}))})
(defn fixture [scenario]
  (let [[_ topology damage] (clojure.string/split (name scenario) #"-")
        edges (m/blocked nodes #{"n1"} topology)
        boots {:n1 "old-1" :n2 "kernel-2" :n3 "kernel-3"}
        rebooted (assoc boots :n1 "new-1")
        rounds (vec (for [[index phase] (map-indexed vector (mapcat #(repeat 2 %) ["partitioned" "powered-off" "restarted-partitioned" "healed"]))
                          :let [expected (inc index) seq (+ 2 index)]]
                      {:phase phase :expected expected :read_floor seq
                       :writes (vec (for [node nodes]
                                      {:node node :service_node node :outcome (if (= "n2" node) "ok" "unknown") :seq seq
                                       :expected expected :id (str node seq) :body (str "v" seq)}))
                       :minority_creates (if (= phase "healed") [] [{:node "n1" :outcome "unknown"}])
                       :reads (vec (for [node nodes] {:node node :service_node node :outcome (if (= "n2" node) "ok" "unknown") :last_seq seq}))}))
        confirmed (into {:1 "seed"} (for [i (range 2 10)] [(keyword (str i)) (str "v" i)]))]
    {:power_events [{:nodes ["n1"] :old_members {:n1 1} :network {:epoch 7 :blocked (vec edges)}}]
     :replicas {:n1 {:member 4} :n2 {:member 2} :n3 {:member 3}}
     :network_faults [{:complete true :topology topology :damage_kind damage :leader "n1"
                       :targets ["n1"] :survivors ["n2" "n3"] :epoch 7 :cut_index 0
                       :membership_before {:voters [1 2 3] :outgoing [] :learners [] :replacement nil}
                       :replicas_before {:n1 {:member 1} :n2 {:member 2} :n3 {:member 3}}
                       :seed "seed" :rounds rounds :confirmed confirmed :audits (zipmap [:n1 :n2 :n3] (repeat confirmed))
                       :canaries [(canary edges boots 7) (canary edges rebooted 7) (canary #{} rebooted 8)]
                       :network_after {:blocked []}
                       :damage [{:node "n1" :old_member 1 :injected damage :voter_started false
                                 :removed_paths ["store/db.node-state"] :store_empty true
                                 :files (if (= damage "wipe") [] [{:path "db" :before {:sha256 (apply str (repeat 64 "0"))}
                                                                 :after {:sha256 (apply str (repeat 64 "1"))}}])}]}]}))

(deftest all-directed-topologies-and-disk-faults-have-positive-controls
  (doseq [scenario m/scenarios]
    (is (true? (m/proof? nodes (fixture scenario) scenario)) (name scenario))))

(deftest faults-cannot-pass-with-fake-links-missing-quorums-or-incomplete-replacement
  (let [p (fixture :vm-split-bitflip)]
    (doseq [[path value]
            [[[:network_faults] []]
             [[:network_faults 0 :complete] false]
             [[:network_faults 0 :targets] ["n1" "n2"]]
             [[:network_faults 0 :membership_before :learners] [4]]
             [[:network_faults 0 :replicas_before :n2 :member] 4]
             [[:power_events 0 :network :blocked] []]
             [[:network_faults 0 :canaries 1 :epoch] 8]
             [[:network_faults 0 :canaries 0 :received] []]
             [[:network_faults 0 :canaries 1 :boot_ids :n1] "old-1"]
             [[:network_faults 0 :canaries 1 :boot_ids :n2] "new-2"]
             [[:network_faults 0 :damage 0 :voter_started] true]
             [[:network_faults 0 :damage 0 :files 0 :after] {:sha256 (apply str (repeat 64 "0"))}]
             [[:replicas :n1 :member] 1]
             [[:network_faults 0 :network_after :blocked] [["n1" "n2"]]]]]
      (is (false? (m/proof? nodes (assoc-in p path value) :vm-split-bitflip)) (str path)))))

(deftest confirmed-forks-stale-reads-and-lost-values-are-rejected
  (let [p (fixture :vm-send-wipe)]
    (doseq [[path value]
            [[[:network_faults 0 :rounds 0 :writes 0 :outcome] "ok"]
             [[:network_faults 0 :rounds 0 :reads 0] {:node "n1" :service_node "n1" :outcome "ok" :last_seq 1}]
             [[:network_faults 0 :rounds 0 :reads 2] {:node "n3" :service_node "n3" :outcome "ok" :last_seq 3}]
             [[:network_faults 0 :rounds 0 :writes 1 :service_node] "n1"]
             [[:network_faults 0 :rounds 0 :minority_creates 0 :outcome] "ok"]
             [[:network_faults 0 :rounds 0 :read_floor] 0]
             [[:network_faults 0 :rounds 1 :expected] 1]
             [[:network_faults 0 :audits :n3 :2] "changed"]
             [[:network_faults 0 :rounds] []]]]
      (is (false? (m/proof? nodes (assoc-in p path value) :vm-send-wipe)) (str path)))
    (let [minority-only (-> p (assoc-in [:network_faults 0 :rounds 0 :writes 0 :outcome] "ok")
                             (assoc-in [:network_faults 0 :rounds 0 :writes 1 :outcome] "unknown"))]
      (is (false? (m/proof? nodes minority-only :vm-send-wipe))))
    (is (false? (m/proof? nodes (assoc-in p [:network_faults 0 :damage 0 :store_empty] false) :vm-send-wipe)))
    (is (false? (m/proof? nodes (assoc-in p [:network_faults 0 :damage 0 :removed_paths] []) :vm-send-wipe)))
    (is (false? (m/recovery? nodes :vm-send-wipe [])))
    (is (true? (m/recovery? nodes :vm-send-wipe
                            (for [node nodes] {:f :exercise :type :ok :value {:node node :recovery true}}))))))
