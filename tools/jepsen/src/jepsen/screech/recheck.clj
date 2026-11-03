(ns jepsen.screech.recheck
  "Replay the consumer checker against saved histories, without starting servers."
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen.checker :as checker]
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
                                             (str "screech-lazyfs-consumer-lifecycle-" (:scenario opts)))
                              run (first (filter #(re-matches #"\d.*" (.getName %)) (.listFiles store)))
                              _ (assert run (str "Missing history for " stem))
                              history (read-history (io/file run "history.edn"))]]
                    {:scenario (:scenario opts)
                     :results (checker/check (consumer/->Checker)
                                             {:nodes (mapv #(str "n" %) (range 1 (inc (:node-count opts))))}
                                             history {})}))
        original (json/parse-string (slurp (io/file root "results.json")) true)
        passed (and (true? (:passed original)) (seq results) (every? #(true? (get-in % [:results :valid?])) results))
        report {:passed (boolean passed) :original-campaign-passed (:passed original) :scenarios results}]
    (spit (io/file root "rechecked-consumers.json") (json/generate-string report {:pretty true}))
    (println (json/generate-string report))
    (shutdown-agents)
    (System/exit (if passed 0 1))))
