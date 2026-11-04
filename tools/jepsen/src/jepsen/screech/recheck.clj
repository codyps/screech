(ns jepsen.screech.recheck
  "Replay the consumer checker against saved histories, without starting servers."
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.screech.cli :as cli]
            [jepsen.screech.mixed :as mixed]
            [jepsen.screech.consumer :as consumer])
  (:import (java.io PushbackReader)))

(defn read-history [path]
  (with-open [reader (PushbackReader. (io/reader path))]
    (loop [events []]
      (let [event (edn/read {:eof ::end} reader)]
        (if (= ::end event) events (recur (conj events event)))))))

(defn -main [root]
  (let [root (io/file root)
        specs (filter #(str/ends-with? (.getName %) ".json") (.listFiles (io/file root "specs")))
        results (vec
                  (for [spec (sort-by #(.getName %) specs)
                        :let [opts (json/parse-string (slurp spec) true)]
                        :when (= "consumer-lifecycle" (:workload opts))
                        :let [stem (str/replace (.getName spec) #"\.json$" "")
                              store (io/file root "runs" stem "store"
                                             (str (if (= "independent-vms" (:topology opts)) "screech-vm-" "screech-lazyfs-")
                                                  "consumer-lifecycle-" (:scenario opts)))
                              run (first (filter #(re-matches #"\d.*" (.getName %)) (.listFiles store)))
                              _ (assert run (str "Missing history for " stem))
                              history (read-history (io/file run "history.edn"))
                              nodes (mapv #(str "n" %) (range 1 (inc (:node-count opts))))
                              native? (= "independent-vms" (:topology opts))
                              proof (when native?
                                      (last (keep #(when (and (= :verify (:f %))
                                                              (get-in % [:value :converged]))
                                                     (:value %)) history)))
                              _ (when native?
                                  (assert (cli/independent-proof? nodes proof)
                                          (str "Invalid independent-VM proof for " stem))
                                  (assert (cli/active-proof? nodes proof (keyword (:scenario opts)))
                                          (str "Invalid active-I/O proof for " stem))
                                  (assert (cli/active-history? nodes proof (keyword (:scenario opts)) history)
                                          (str "Missing active requests or completed recovery cycles for " stem))
                                  (assert (and (mixed/proof? nodes proof (keyword (:scenario opts)))
                                               (mixed/recovery? nodes (keyword (:scenario opts)) history))
                                          (str "Invalid mixed network/disk proof for " stem))
                                  (spit (io/file root "runs" stem "convergence.json")
                                        (json/generate-string proof {:pretty true})))]]
                    {:scenario (:scenario opts)
                     :independent-vms-verified (when native? true)
                     :results (checker/check (consumer/->Checker)
                                             {:nodes nodes}
                                             history {})}))
        original (json/parse-string (slurp (io/file root "results.json")) true)
        passed (and (true? (:passed original)) (seq results) (every? #(true? (get-in % [:results :valid?])) results))
        report {:passed (boolean passed) :original-campaign-passed (:passed original) :scenarios results}]
    (spit (io/file root "rechecked-consumers.json") (json/generate-string report {:pretty true}))
    (println (json/generate-string report))
    (shutdown-agents)
    (System/exit (if passed 0 1))))
