(ns jepsen.screech.consumer
  "Consumer durability histories. Each cycle owns its streams; audits share only
  an address catalog. The checker derives obligations from recorded API results."
  (:refer-clojure :exclude [bytes])
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.screech.client :as c])
  (:import (io.nats.client Connection Message)
           (io.nats.client.impl Headers)
           (java.time Duration)
           (java.util Base64 UUID)))

(defn bytes [s] (.getBytes ^String s java.nio.charset.StandardCharsets/UTF_8))
(defn text [data] (String. ^bytes data java.nio.charset.StandardCharsets/UTF_8))
(defn unavailable [message] (throw (ex-info message {:uncertain true})))
(defn request [^Connection conn subject payload headers timeout]
  (let [message (.request conn ^String subject ^Headers headers ^bytes payload (Duration/ofMillis timeout))]
    (when (and message (.isStatusMessage message))
      (unavailable (str "NATS status " (.getStatus message))))
    message))
(defn api [conn subject body]
  (let [message (request conn subject (bytes (json/generate-string body)) nil 3000)]
    (when-not message (unavailable (str "Timeout: " subject)))
    (let [result (json/parse-string (text (.getData ^Message message)) true)]
      (when (>= (get-in result [:error :code] 0) 500)
        (unavailable (str "API unavailable: " result)))
      result)))
(defn success [result]
  (when (:error result) (throw (ex-info "API rejected operation" {:response result :violation true})))
  result)
(defn step! [journal kind fields f]
  ;; Keep completed steps even when the enclosing cycle later has an unknown outcome.
  (let [id (count @journal)]
    (swap! journal conj (merge fields {:kind :attempt :action kind :request id}))
    (let [result (f)]
      (swap! journal conj (merge fields result {:kind kind :request id}))
      result)))
(defn stream! [conn journal name retention replicas]
  (step! journal :stream {:stream name :retention retention}
         #(do (success (api conn (str "$JS.API.STREAM.CREATE." name)
                            {:name name :subjects [(str name ".data")] :storage "file"
                             :num_replicas replicas :retention retention
                             :duplicate_window 86400000000000})) {})))
(defn consumer! [conn journal stream name epoch]
  (step! journal :consumer {:stream stream :consumer name :epoch epoch}
         #(do (success (api conn (str "$JS.API.CONSUMER.DURABLE.CREATE." stream "." name)
                            {:stream_name stream :config {:durable_name name :ack_policy "explicit"
                                                         :ack_wait 3600000000000 :max_ack_pending 100
                                                         :deliver_policy "all" :replay_policy "instant"}})) {})))
(defn publish! [conn journal stream role]
  (let [id (str stream ":" (name role)) body id]
    (step! journal :publish {:stream stream :role role :body body :id id}
           #(let [headers (doto (Headers.) (.add "Nats-Msg-Id" (into-array String [id])))
                  response (request conn (str stream ".data") (bytes body) headers 3000)]
              (when-not response (unavailable "Publication timeout"))
              (success (json/parse-string (text (.getData ^Message response)) true))))))
(defn deliver! [conn journal stream consumer epoch]
  (step! journal :deliver {:stream stream :consumer consumer :epoch epoch}
         #(let [inbox (.createInbox ^Connection conn)
                subscription (.subscribe ^Connection conn inbox)]
            ;; Pull deliveries carry the original subject. A multiplexed request
            ;; inbox would discard them; use a dedicated synchronous subscription.
            (try
              (.publish ^Connection conn (str "$JS.API.CONSUMER.MSG.NEXT." stream "." consumer)
                        inbox (bytes "{\"batch\":1,\"no_wait\":true}"))
              (let [message (.nextMessage subscription (Duration/ofSeconds 3))]
                (when-not message (unavailable "Delivery timeout"))
                (when (.isStatusMessage ^Message message) (unavailable (str (.getStatus ^Message message))))
                (let [token (.getReplyTo ^Message message) parts (str/split (or token "") #"\.")]
                  (when-not (= 12 (count parts)) (throw (ex-info "Invalid delivery token" {:violation true :token token})))
                  {:token token :seq (parse-long (nth parts 7)) :cseq (parse-long (nth parts 8))
                   :generation (nth parts 11) :body (text (.getData ^Message message))}))
              (finally (.unsubscribe subscription))))))

(defn ack! [conn journal stream consumer epoch delivery]
  (step! journal :ack (merge (select-keys delivery [:seq :cseq :generation])
                             {:stream stream :consumer consumer :epoch epoch})
         #(let [response (request conn (:token delivery) (bytes "+ACK") nil 3000)]
            (when-not response (unavailable "ACK outcome unknown: no confirmation"))
            (when-not (zero? (alength (.getData ^Message response)))
              (throw (ex-info "Invalid ACK confirmation" {:violation true})))
            {:confirmed true})))
(defn duplicate!
  ([conn stream] (duplicate! conn stream :stable))
  ([conn stream role]
  (let [headers (doto (Headers.) (.add "Nats-Msg-Id" (into-array String [(str stream ":" (name role))])))
        response (request conn (str stream ".data") (bytes "must-not-replace-original") headers 3000)]
    (when-not response (unavailable "Duplicate retry outcome unknown"))
    (success (json/parse-string (text (.getData ^Message response)) true)))))
(defn stale! [conn token]
  ;; A timeout is not proof of ignoring the ACK. Subsequent payload/offset reads
  ;; and the final audits check its effect. An empty confirmation is a violation.
  (let [response (request conn token (bytes "+ACK") nil 150)]
    {:confirmed (boolean response)}))

(defn exercise! [conn catalog journal case replicas]
  (let [{:keys [id streams]} case w (:w streams) l (:l streams) i (:i streams)]
    (stream! conn journal l "limits" replicas)
    (consumer! conn journal l "D" 0)
    (consumer! conn journal l "GONE" 0)
    (step! journal :delete {:stream l :consumer "GONE" :epoch 0}
           #(do (success (api conn (str "$JS.API.CONSUMER.DELETE." l ".GONE") {})) {}))
    (publish! conn journal l :stable)
    (swap! catalog assoc-in [id :dedup] true)
    (ack! conn journal l "D" 0 (deliver! conn journal l "D" 0))
    (step! journal :dedup {:stream l} #(duplicate! conn l))
    (stream! conn journal i "interest" replicas)
    (consumer! conn journal i "D" 0)
    (consumer! conn journal i "E" 0)
    (publish! conn journal i :removed)
    (ack! conn journal i "D" 0 (deliver! conn journal i "D" 0))
    (ack! conn journal i "E" 0 (deliver! conn journal i "E" 0))
    (publish! conn journal i :pending)
    (ack! conn journal i "D" 0 (deliver! conn journal i "D" 0))
    (deliver! conn journal i "E" 0)
    (stream! conn journal w "workqueue" replicas)
    (consumer! conn journal w "D" 0)
    (publish! conn journal w :removed)
    (swap! catalog assoc-in [id :work-dedup] true)
    (ack! conn journal w "D" 0 (deliver! conn journal w "D" 0))
    (publish! conn journal w :pending)
    (let [old (deliver! conn journal w "D" 0)]
      (step! journal :delete {:stream w :consumer "D" :epoch 0}
             #(let [r (success (api conn (str "$JS.API.CONSUMER.DELETE." w ".D") {}))]
                (when-not (:success r) (throw (ex-info "Deletion not confirmed" {:violation true}))) {}))
      (consumer! conn journal w "D" 1)
      (deliver! conn journal w "D" 1)
      (swap! catalog assoc-in [id :stale-token] (:token old))
      (step! journal :stale {:stream w} #(stale! conn (:token old))))
    (swap! journal conj {:kind :complete})))

(defn read-stream [conn stream consumers seqs journal id]
  (let [record (fn [path result]
                 (swap! journal conj {:kind :read :case id :stream stream :path path :result result})
                 result)]
    {:info (record [:info] (api conn (str "$JS.API.STREAM.INFO." stream) {}))
     :consumers (into {} (for [name consumers]
                          [name (record [:consumers name] (api conn (str "$JS.API.CONSUMER.INFO." stream "." name) {}))]))
     :messages (into {} (for [seq seqs]
                          [seq (record [:messages seq]
                                       (let [r (api conn (str "$JS.API.STREAM.MSG.GET." stream) {:seq seq})]
                                         (if-let [message (:message r)]
                                           {:body (text (.decode (Base64/getDecoder) ^String (:data message)))
                                            :seq (:seq message)}
                                           r)))]))}))

(defn audit! [conn catalog journal]
  (into {} (for [[id {:keys [streams dedup work-dedup stale-token]}] (sort-by key @catalog)]
             [id (let [d (when dedup (step! journal :dedup {:stream (:l streams) :case id} #(duplicate! conn (:l streams))))
                       wd (when work-dedup (step! journal :dedup {:stream (:w streams) :case id} #(duplicate! conn (:w streams) :removed)))
                       stale (when stale-token (step! journal :stale {:stream (:w streams) :case id} #(stale! conn stale-token)))]
                   {:dedup d :work-dedup wd :stale stale
                    :streams {(:l streams) (read-stream conn (:l streams) ["D" "GONE"] [1] journal id)
                              (:i streams) (read-stream conn (:i streams) ["D" "E"] [1 2] journal id)
                              (:w streams) (read-stream conn (:w streams) ["D"] [1 2] journal id)}})])))

(defrecord Client [connection node catalog]
  client/Client
  (open! [this test node] (assoc this :connection (:conn (c/open test node)) :node node))
  (setup! [_ _])
  (invoke! [_ test op]
    (let [journal (atom []) id (str/replace (str (UUID/randomUUID)) "-" "")
          fixture {:id id :seed (:seed (:value op)) :streams {:w (str "W_" id) :l (str "L_" id) :i (str "I_" id)}}
          value (if (= :exercise (:f op)) fixture {:node node})]
      (try
        (let [result (case (:f op)
                       :exercise (do (swap! catalog assoc id fixture)
                                     (exercise! connection catalog journal fixture (count (:nodes test))) {})
                       :audit {:cases (audit! connection catalog journal)})]
          (assoc op :type :ok :value (merge value result {:events @journal})))
        (catch Exception error
          (let [uncertain (or (:uncertain (ex-data error))
                              (instance? java.io.IOException error)
                              (instance? java.util.concurrent.TimeoutException error)
                              (instance? InterruptedException error))]
          (assoc op :type :info :value (assoc value :events @journal)
                 :error (merge {:message (.getMessage error) :class (.getName (class error))
                                :violation (not uncertain)} (ex-data error))))))))
  (teardown! [_ _])
  (close! [_ _] (when connection (.close ^Connection connection)))
  client/Reusable
  (reusable? [_ _] true))

(defn completed-events [history]
  (mapcat #(when (and (= :exercise (:f %)) (#{:ok :info} (:type %)))
             (get-in % [:value :events])) history))
(defn acknowledged [history] (count (filter #(= :publish (:kind %)) (completed-events history))))

(defn absent? [result] (= 404 (get-in result [:error :code])))
(defn issues [events observed replicas complete?]
  (let [errors (atom [])
        demand (fn [condition kind detail] (when-not condition (swap! errors conj {:kind kind :detail detail})))
        partial? (:partial? observed)
        streams (filter #(and (= :stream (:kind %))
                              (or (not partial?) (contains? (:streams observed) (:stream %)))) events)]
    (doseq [{:keys [stream retention]} streams]
      (let [{:keys [info consumers messages]} (get-in observed [:streams stream])
            es (filter #(= stream (:stream %)) events)
            pubs (filter #(= :publish (:kind %)) es)
            acks (filter #(= :ack (:kind %)) es)
            attempts (filter #(and (= :attempt (:kind %)) (= :ack (:action %))) es)]
        (when (or (not partial?) (contains? (get-in observed [:streams stream]) :info))
        (demand (and info (not (:error info))) :lost-stream stream)
        (demand (= retention (get-in info [:config :retention])) :retention-config stream)
        (demand (= replicas (get-in info [:config :num_replicas])) :replica-config stream))
        (doseq [{:keys [seq body role]} pubs :when (or (not partial?) (contains? messages seq))]
          (let [expected-seq (if (= role :pending) 2 1)
                acked (set (map :consumer (filter #(= seq (:seq %)) acks)))
                attempted (set (map :consumer (filter #(= seq (:seq %)) attempts)))
                removed (case retention "workqueue" (clojure.core/seq acked) "interest" (= #{"D" "E"} acked) false)
                uncertain (case retention "workqueue" (clojure.core/seq attempted) "interest" (= #{"D" "E"} attempted) false)
                actual (get messages seq)]
            (demand (= expected-seq seq) :wrong-publication-sequence {:stream stream :seq seq})
            (cond removed (demand (absent? actual) :retention-resurrected {:stream stream :seq seq})
                  uncertain (demand (or (absent? actual) (= body (:body actual))) :wrong-payload {:stream stream :seq seq})
                  :else (demand (= body (:body actual)) :lost-or-changed-payload {:stream stream :seq seq}))))
        (doseq [name (distinct (map :consumer (filter #(= :consumer (:kind %)) es)))
                :when (or (not partial?) (contains? consumers name))]
          (let [changes (filter #(and (= name (:consumer %)) (#{:consumer :delete} (:kind %))) es)
                latest (last changes)
                maybe-change (some #(and (= :attempt (:kind %)) (= name (:consumer %))
                                         (#{:delete :consumer} (:action %)) (> (:request %) (:request latest))) es)
                current (get consumers name)]
            (when-not maybe-change
              (if (= :delete (:kind latest))
                (demand (absent? current) :resurrected-consumer {:stream stream :consumer name})
                (do
                  (demand (and current (not (:error current))) :lost-consumer {:stream stream :consumer name})
                  (demand (= "explicit" (get-in current [:config :ack_policy])) :ack-policy-reset {:stream stream :consumer name})
                  (demand (= 3600000000000 (get-in current [:config :ack_wait])) :ack-wait-reset {:stream stream :consumer name})
                  (doseq [ack (filter #(and (= name (:consumer %)) (= (:epoch latest) (:epoch %))) acks)]
                    (demand (<= (:cseq ack) (get-in current [:ack_floor :consumer_seq] -1))
                            :confirmed-ack-rollback {:stream stream :consumer name :ack ack :info current}))
                  (let [deliveries (filter #(and (= :deliver (:kind %)) (= name (:consumer %))
                                                  (= (:epoch latest) (:epoch %))) es)
                        unacked (remove #(some (fn [ack] (= (:seq %) (:seq ack)))
                                               (filter (fn [ack] (= name (:consumer ack))) acks)) deliveries)]
                    (when complete?
                      (demand (= (count (set (map :seq unacked))) (:num_ack_pending current))
                              :wrong-pending-count {:stream stream :consumer name :info current}))
                    (doseq [delivery deliveries]
                      (demand (<= (:cseq delivery) (get-in current [:delivered :consumer_seq] -1))
                              :delivery-reservation-rollback {:stream stream :consumer name :delivery delivery}))
                    (when (and (seq unacked)
                               (not-any? (fn [a] (and (= name (:consumer a)) (= (:epoch latest) (:epoch a))
                                                      (some #(= (:seq a) (:seq %)) unacked))) attempts))
                      (demand (pos? (:num_ack_pending current 0)) :lost-reservation {:stream stream :consumer name})))
                  (when (and (= "workqueue" retention) (= 1 (:epoch latest))
                             (some #(and (= :deliver (:kind %)) (= 1 (:epoch %))) es))
                    (demand (zero? (get-in current [:ack_floor :consumer_seq] -1)) :stale-ack-advanced-floor stream)
                    (demand (= 1 (:num_ack_pending current)) :stale-ack-removed-reservation stream)))))))
        (when (and (or (not partial?) (contains? observed :work-dedup))
                   (= "workqueue" retention) (some #(= :removed (:role %)) pubs))
          (demand (and (true? (get-in observed [:work-dedup :duplicate]))
                       (= 1 (get-in observed [:work-dedup :seq]))) :dedup-after-retention-rollback stream))
        (when (and (= "limits" retention) (seq pubs))
          (when (or (not partial?) (contains? observed :dedup))
          (demand (and (true? (get-in observed [:dedup :duplicate]))
                       (= (:seq (first pubs)) (get-in observed [:dedup :seq]))) :dedup-rollback stream))
          (when (or (not partial?) (contains? (get-in observed [:streams stream]) :info))
          (demand (= 1 (get-in info [:state :messages])) :duplicate-created-record stream)
          (demand (= 1 (get-in info [:state :last_seq])) :duplicate-advanced-sequence stream)))
        (when (and complete? (or (not partial?) (contains? (get-in observed [:streams stream]) :info)))
          (demand (= 1 (get-in info [:state :messages])) :wrong-retained-count stream))))
    (doseq [ack (filter #(= :ack (:kind %)) events)]
      (demand (true? (:confirmed ack)) :unconfirmed-ack-labelled-success ack))
    (doseq [delivery (filter #(= :deliver (:kind %)) events)]
      (let [publication (some #(when (and (= :publish (:kind %)) (= (:stream delivery) (:stream %))
                                         (= (:seq delivery) (:seq %))) %) events)]
        (demand (= (:body publication) (:body delivery)) :wrong-delivery delivery)))
    (let [old (some #(when (and (= :deliver (:kind %)) (= 0 (:epoch %)) (= 2 (:seq %))
                                (str/starts-with? (:stream %) "W_")) %) events)
          new (some #(when (and (= :deliver (:kind %)) (= 1 (:epoch %))) %) events)]
      (when new
        (demand (= 2 (:seq new)) :wrong-recreated-delivery new)
        (demand (not= (:generation old) (:generation new)) :reused-consumer-generation new)))
    (doseq [e events :when (#{:dedup :stale} (:kind e))]
      (if (= :stale (:kind e))
        (demand (false? (:confirmed e)) :stale-ack-confirmed e)
        (demand (and (true? (:duplicate e)) (= 1 (:seq e))) :dedup-failed e)))
    (when (:stale observed) (demand (false? (get-in observed [:stale :confirmed])) :stale-ack-confirmed-after-recovery observed))
    @errors))

(defrecord Checker []
  checker/Checker
  (check [_ test history _]
    (let [cycles (filter #(and (= :exercise (:f %)) (#{:ok :info} (:type %))) history)
          cases (into {} (map (fn [op] [(get-in op [:value :id]) (:value op)]) cycles))
          audits (filter #(and (= :audit (:f %)) (= :ok (:type %))) history)
          by-node (group-by #(get-in % [:value :node]) audits)
          errors (vec (concat
                        (for [node (:nodes test) :when (empty? (by-node node))] {:kind :missing-audit :node node})
                        (for [op history :when (get-in op [:error :violation])] {:kind :protocol-violation :error (:error op)})
                        (for [op history :when (#{:ok :info} (:type op))
                              event (get-in op [:value :events])
                              :when (or (and (= :stale (:kind event)) (:confirmed event))
                                        (and (= :dedup (:kind event))
                                             (not (and (true? (:duplicate event)) (= 1 (:seq event))))))]
                          {:kind :invalid-probe-result :node (get-in op [:value :node]) :event event})
                        (mapcat (fn [op]
                                  (for [event (get-in op [:value :events]) :when (= :read (:kind event))
                                        :let [fixture (cases (:case event))
                                              partial (assoc-in {:partial? true} (into [:streams (:stream event)] (:path event)) (:result event))]
                                        issue (issues (:events fixture) partial (count (:nodes test))
                                                      (boolean (some #(= :complete (:kind %)) (:events fixture))))]
                                    (assoc issue :node (get-in op [:value :node]) :case (:case event))))
                                (filter #(and (= :audit (:f %)) (#{:ok :info} (:type %))) history))
                        (mapcat (fn [audit]
                                  (mapcat (fn [[id {:keys [events]}]]
                                            (map #(assoc % :node (get-in audit [:value :node]) :case id)
                                                 (issues events (get-in audit [:value :cases id]) (count (:nodes test))
                                                         (boolean (some #(= :complete (:kind %)) events))))) cases)) audits)))
          events (completed-events history)
          counts (frequencies (map :kind events))
          seeded (count (filter #(and (:seed %) (some (fn [e] (= :complete (:kind e))) (:events %))) (vals cases)))
          coverage (and (pos? seeded) (every? #(pos? (get counts % 0)) [:publish :ack :delete :consumer :dedup :stale]))]
      {:valid? (boolean (and coverage (empty? errors))) :seeded-cycles seeded :cycles (count cases)
       :confirmed-acks (get counts :ack 0) :acknowledged-publications (get counts :publish 0)
       :events counts :audited-nodes (sort (keys by-node)) :error-count (count errors) :errors (vec (take 32 errors))})))

(defn workload [_]
  (let [catalog (atom {})]
    {:initial-generator (gen/each-thread (gen/once {:f :exercise :value {:seed true}}))
     :generator (gen/each-thread (gen/limit 2 (gen/repeat {:f :exercise})))
     :final-generator (gen/each-thread (gen/until-ok (gen/repeat {:f :audit})))
     :wrap-generator identity :client (->Client nil nil catalog) :checker (->Checker)}))
