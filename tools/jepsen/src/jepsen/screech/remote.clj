(ns jepsen.screech.remote
  "Jepsen control over a loopback-only RPC connection to the disposable VM."
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [jepsen.control.core :as core])
  (:import (java.net URI HttpURLConnection)
           (java.util Base64)))

(defprotocol Guest
  (rpc [this op node payload]))

(deftype RPCRemote [endpoint token host]
  Guest
  (rpc [_ op node payload]
    (let [connection ^HttpURLConnection (.openConnection (.toURL (URI. endpoint)))]
      (.setRequestMethod connection "POST")
      (.setConnectTimeout connection 5000)
      (.setReadTimeout connection (if (= op "mixed-fault") 300000 120000))
      (.setRequestProperty connection "Authorization" (str "Bearer " token))
      (.setDoOutput connection true)
      (with-open [writer (io/writer (.getOutputStream connection))]
        (.write writer (json/generate-string (merge payload {:op op :node node}))))
      (let [response (json/parse-string (slurp (.getInputStream connection)) true)]
        (.disconnect connection)
        (when-let [error (:error response)] (throw (ex-info "VM control failed" response)))
        response)))
  core/Remote
  (connect [_ spec] (RPCRemote. endpoint token (:host spec)))
  (disconnect! [_])
  (execute! [this _ action]
    (merge action (rpc this "exec" host {:cmd (:cmd action) :in (or (:in action) "")})))
  (upload! [this _ paths destination _]
    (assert (= 1 (count paths)))
    (rpc this "write" host {:path destination :data (.encodeToString (Base64/getEncoder) (java.nio.file.Files/readAllBytes (.toPath (io/file (first paths)))))}))
  (download! [this _ source destination _]
    (let [response (rpc this "read" host {:path source})]
      (io/make-parents destination)
      (with-open [out (io/output-stream destination)]
        (.write out (.decode (Base64/getDecoder) ^String (:data response)))))))

(defn remote [endpoint]
  (assert (= "127.0.0.1" (.getHost (URI. endpoint))) "VM control must use a loopback endpoint")
  (let [remote (RPCRemote. endpoint (System/getenv "SCREECH_JEPSEN_CONTROL_TOKEN") nil)]
    (assert (:isolated-vm (rpc remote "status" nil {})) "fault controls require the isolated guest")
    remote))
