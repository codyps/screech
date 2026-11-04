(ns jepsen.screech.mixed
  "Replay network controls and conditional-write/read witnesses independently."
  (:require [clojure.string :as str]))

(def scenarios (set (for [network ["split" "send" "receive"] damage ["bitflip" "wipe"]]
                      (keyword (str "vm-" network "-" damage)))))
(defn pairs [nodes] (set (for [a nodes b nodes :when (not= a b)] [a b])))
(defn blocked [nodes targets topology]
  (set (for [[a b] (pairs nodes)
             :when (or (and (#{"split" "send"} topology) (targets a) (not (targets b)))
                       (and (#{"split" "receive"} topology) (not (targets a)) (targets b)))] [a b])))
(defn number-map [m] (into {} (map (fn [[k v]] [(parse-long (name k)) v]) m)))
(defn node-map [m] (into {} (map (fn [[k v]] [(name k) v]) m)))

(defn canary? [nodes blocked c]
  (let [counts (into {} (map (fn [e] [[(:source e) (:destination e)] e]) (:edges c)))
        allowed (remove blocked (pairs nodes))]
    (and (= (pairs nodes) (set (:attempted c))) (= blocked (set (:blocked c)))
         (= (set allowed) (set (:received c)))
         (= (set nodes) (set (keys (node-map (:boot_ids c)))))
         (= (count nodes) (count (set (vals (:boot_ids c)))))
         (every? #(and (string? %) (seq %)) (vals (:boot_ids c)))
         (every? #(pos? (get-in counts [% :dropped] 0)) blocked)
         (every? #(pos? (get-in counts [% :forwarded] 0)) allowed))))

(defn witness? [nodes c]
  (let [targets (set (:targets c)) survivors (set (:survivors c)) rounds (:rounds c)
        result (reduce
                (fn [state r]
                  (when state
                    (let [known (:known state) observed (:observed state)
                          writes (:writes r) reads (:reads r) successes (filter #(= "ok" (:outcome %)) writes)
                          creates (:minority_creates r)
                          floor (apply max (concat [observed (:expected r)] (map :seq successes)))
                          next (reduce #(assoc %1 (:seq %2) (:body %2)) known successes)]
                      (when (and (>= (:expected r) observed)
                                 (= (set nodes) (set (map :node writes))) (= (count nodes) (count writes))
                                 (= (set nodes) (set (map :node reads))) (= (count nodes) (count reads))
                                 (= (if (= "healed" (:phase r)) #{} targets) (set (map :node creates)))
                                 (= (if (= "healed" (:phase r)) 0 (count targets)) (count creates))
                                 (not-any? #(= "ok" (:outcome %)) creates)
                                 (<= (count successes) 1)
                                 (every? #(and (= (inc (:expected r)) (:seq %))
                                               (= (:node %) (:service_node %))
                                               (= (:expected r) (:expected %))
                                               (string? (:id %)) (seq (:id %)) (string? (:body %))
                                               (or (= "healed" (:phase r)) (survivors (:node %)))
                                               (or (not (contains? known (:seq %))) (= (known (:seq %)) (:body %)))) successes)
                                 (= floor (:read_floor r))
                                 (every? #(or (not= "ok" (:outcome %))
                                              (and (= (:node %) (:service_node %)) (>= (:last_seq %) floor))) reads))
                        {:known next :observed (apply max floor (map :last_seq (filter #(= "ok" (:outcome %)) reads)))}))))
                {:known {1 (:seed c)} :observed 1} rounds)
        known (:known result)]
    (and result (= known (number-map (:confirmed c)))
         (every? (fn [phase]
                   (<= 2 (count (filter #(and (= phase (:phase %))
                                              (some (fn [w] (and (= "ok" (:outcome w)) (survivors (:node w)))) (:writes %))) rounds))))
                 ["partitioned" "powered-off" "restarted-partitioned" "healed"])
         (= (set nodes) (set (keys (node-map (:audits c)))))
         (every? #(= known (number-map %)) (vals (:audits c))))))

(defn proof? [nodes proof scenario]
  (if-not (scenarios scenario) true
    (try
      (let [[_ topology damage] (str/split (name scenario) #"-")
            cases (:network_faults proof) c (first cases) targets (set (:targets c)) survivors (set (:survivors c))
            edges (blocked nodes targets topology) cs (:canaries c) [before rebooted healed] cs
            cut (get (:power_events proof) (:cut_index c))
            membership (:membership_before c) replicas (:replicas_before c)
            before-ids (node-map (:boot_ids before)) rebooted-ids (node-map (:boot_ids rebooted))]
        (boolean
          (and (= 1 (count cases)) (true? (:complete c)) (= topology (:topology c)) (= damage (:damage_kind c))
               (targets (:leader c)) (seq targets) (<= (count targets) (quot (count nodes) 2))
               (= (set nodes) (into targets survivors)) (not-any? targets survivors)
               (>= (count survivors) (inc (quot (count nodes) 2)))
               (= (set nodes) (set (keys (node-map replicas))))
               (= (count nodes) (count (:voters membership)))
               (= (set (:voters membership)) (set (map :member (vals replicas))))
               (empty? (:outgoing membership)) (empty? (:learners membership)) (nil? (:replacement membership))
               (= targets (set (:nodes cut))) (= edges (set (get-in cut [:network :blocked])))
               (= (:epoch c) (:epoch before) (:epoch rebooted) (get-in cut [:network :epoch]))
               (= 3 (count cs)) (canary? nodes edges before) (canary? nodes edges rebooted) (canary? nodes #{} healed)
               (= (:boot_ids healed) (:boot_ids rebooted))
               (every? #(not= (before-ids %) (rebooted-ids %)) targets)
               (every? #(= (before-ids %) (rebooted-ids %)) survivors)
               (empty? (get-in c [:network_after :blocked]))
               (= targets (set (map :node (:damage c)))) (= (count targets) (count (:damage c)))
               (every? (fn [d]
                         (and (= damage (:injected d)) (false? (:voter_started d))
                              (= (:old_member d) (get (:old_members cut) (keyword (:node d))))
                              (not= (:old_member d) (get-in proof [:replicas (keyword (:node d)) :member]))
                              (if (= "wipe" damage)
                                (and (true? (:store_empty d)) (seq (:removed_paths d))
                                     (some #(str/ends-with? % "db.node-state") (:removed_paths d)))
                                (and (seq (:files d))
                                     (every? #(and (string? (:path %))
                                                   (= 64 (count (get-in % [:before :sha256])))
                                                   (= 64 (count (get-in % [:after :sha256])))
                                                   (not= (get-in % [:before :sha256]) (get-in % [:after :sha256]))) (:files d)))))) (:damage c))
               (witness? nodes c))))
      (catch Exception _ false))))

(defn recovery? [nodes scenario history]
  (or (not (scenarios scenario))
      (= (set nodes) (set (keep #(when (and (= :exercise (:f %)) (= :ok (:type %))
                                              (get-in % [:value :recovery])) (get-in % [:value :node])) history)))))
