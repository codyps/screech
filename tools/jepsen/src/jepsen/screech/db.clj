(ns jepsen.screech.db
  "Each voter has its own guest namespace, LazyFS mount, backing directory and process."
  (:require [jepsen.db :as db]
            [jepsen.screech.remote :as remote]))

(def root "/data/screech-jepsen")
(defn node-dir [node] (str root "/" node))
(defn action [test op node payload] (remote/rpc (:remote test) op node payload))

(defrecord DB [stats]
  db/DB
  (setup! [this test node]
    (let [nodes (:nodes test)
          id (inc (.indexOf ^java.util.List nodes node))
          peers (into {} (map-indexed (fn [i _]
                                      [(str (inc i)) {:raft (str "10.0.1." (inc i) ":7000")
                                                     :client (str "10.0.1." (inc i) ":4222")
                                                     :service (str "10.0.1." (inc i) ":7500")}]) nodes))]
      (action test "setup" node {:binary (:binary test) :lazyfs (:lazyfs-binary test)
                                :config {:cluster_id (:cluster-id test) :node_id id
                                         :secret (:cluster-secret test) :peers peers
                                         :initial_voters (vec (range 1 (inc (count nodes))))}})
      (swap! stats update (if (= "independent-vms" (:topology test)) :vm-nodes :canaries) (fnil inc 0))))
  (teardown! [_ test node] (action test "teardown" node {}))
  db/LogFiles
  (log-files [_ test node]
    (if (= "independent-vms" (:topology test))
      {(str (node-dir node) "/server.log") "server.log"
       (str (node-dir node) "/power-canary.json") "power-canary.json"
       (str (node-dir node) "/convergence.json") "convergence.json"}
    {(str (node-dir node) "/server.log") "server.log"
     (str (node-dir node) "/lazyfs.log") "lazyfs.log"
     (str (node-dir node) "/lazyfs-stdout.log") "lazyfs-stdout.log"
     (str (node-dir node) "/canary.json") "canary.json"
     (str (node-dir node) "/convergence.json") "convergence.json"}))
  db/Primary
  (primaries [_ _] [])
  (setup-primary! [_ test node]
    (action test (if (= "independent-vms" (:topology test)) "preflight" "ready") node {:nodes (:nodes test)}))
  db/Kill
  (kill! [_ test node]
    (let [result (action test "kill" node {})]
      (if (= "independent-vms" (:topology test))
        (do (assert (:guest_killed result)) (swap! stats update :vm-cuts (fnil inc 0)))
        (do (assert (:cache_clear_acknowledged result)) (swap! stats update :cache-clears (fnil inc 0))))
      result))
  (start! [_ test node] (action test "start" node {}))
  db/Pause
  (pause! [_ test node] (action test "pause" node {}))
  (resume! [_ test node] (action test "resume" node {})))

(defn database [] (->DB (atom {})))
