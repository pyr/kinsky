(ns kinsky.client
  "Small clojure shim on top of the Kafka client API
   See https://github.com/pyr/kinsky for example usage."
  (:require [clojure.edn           :as edn]
            [jsonista.core         :as json])
  (:import (java.util Collection)
           (java.util Map)
           (java.util.concurrent TimeUnit)
           (java.util.regex Pattern)
           (org.apache.kafka.clients.consumer ConsumerRebalanceListener
                                              ConsumerRecord
                                              ConsumerRecords
                                              KafkaConsumer
                                              OffsetAndMetadata)
           (org.apache.kafka.clients.producer Callback
                                              KafkaProducer
                                              ProducerRecord
                                              RecordMetadata)
           (org.apache.kafka.common Node
                                    PartitionInfo
                                    TopicPartition)
           (org.apache.kafka.common.errors WakeupException)
           (org.apache.kafka.common.serialization Deserializer
                                                  Serializer
                                                  StringDeserializer
                                                  StringSerializer)
           (org.apache.kafka.common.header Header
                                           Headers)))

(defprotocol MetadataDriver
  "Common properties for all drivers"
  (partitions-for [this topic]
    "Retrieve partition ownership information for a topic.
     The result is a data representation of a
     [PartitionInfo](http://kafka.apache.org/090/javadoc/org/apache/kafka/common/PartitionInfo.html)
     list.
     The structure for a partition info map is:

         {:topic     \"t\"
          :partition 0
          :isr       [{:host \"x\" :id 0 :port 9092}]
          :leader    {:host \"x\" :id 0 :port 9092}
          :replicas  [{:host \"x\" :id 0 :port 9092}]"))

(defprotocol ConsumerDriver
  "Driver interface for consumers"
  (poll!          [this timeout]
    "Poll for new messages. Timeout in ms.
     The result is a data representation of a ConsumerRecords instance.

         {:partitions [[\"t\" 0] [\"t\" 1]]
          :topics     [\"t\"]
          :count      2
          :by-partition {[\"t\" 0] [{:key       \"k0\"
                                     :offset    1
                                     :partition 0
                                     :timestamp 1233524522
                                     :topic     \"t\"
                                     :value     \"v0\"
                                     :headers   {:h1 [\"1\" \"2\"]}}]
                         [\"t\" 1] [{:key       \"k1\"
                                     :offset    1
                                     :partition 1
                                     :timestamp 1233524527
                                     :topic     \"t\"
                                     :value     \"v1\"}
                                     :headers   {:h1 [\"1\"]}]}
          :by-topic      {\"t\" [{:key       \"k0\"
                                  :offset    1
                                  :partition 0
                                  :timestamp 1233524522
                                  :topic     \"t\"
                                  :value     \"v0\"
                                  :headers   {:h1 [\"1\" \"2\"]}}
                                 {:key       \"k1\"
                                  :offset    1
                                  :partition 1
                                  :timestamp 1233524527
                                  :topic     \"t\"
                                  :value     \"v1\"
                                  :headers   {:h1 [\"1\"]}}]}}")
  (stop!          [this] [this timeout]
    "Stop consumption.")
  (pause!         [this] [this topic-partitions]
    "Pause consumption.")
  (resume!        [this topic-partitions]
    "Resume consumption.")
  (unsubscribe!   [this]
    "Unsubscribe from currently subscribed topics.")
  (subscribe!     [this topics] [this topics listener]
    "Subscribe to a topic or list of topics.
     The topics argument can be:

     - A simple string or keyword when subscribing to a single topic
     - A regex pattern to subscribe to matching topics
     - A collection of strings or keywords

     The optional listener argument is either a callback
     function or an implementation of
     [ConsumerRebalanceListener](http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/ConsumerRebalanceListener.html).

     When a function is supplied, it will be called on rebalance
     events with a map representing the event, see
     [kinsky.client/rebalance-listener](#var-rebalance-listener)
     for details on the map format.")
  (commit!         [this] [this topic-offsets]
    "Commit offsets for a consumer.
     The topic-offsets argument must be a list of maps of the form:

     ```
     {:topic     topic
      :partition partition
      :offset    offset
      :metadata  metadata}
     ```
     The topic and partition tuple must be unique across the whole list.")
  (wake-up!        [this]
    "Safely wake-up a consumer which may be blocking during polling.")
  (seek!           [this] [this topic-partition offset]
    "Overrides the fetch offsets that the consumer will use on the next poll")
  (position!      [this] [this topic-partition]
    "Get the offset of the next record that will be fetched (if a record with that offset exists).")
  (subscription   [this]
    "Currently assigned topics"))

(defprotocol ProducerDriver
  "Driver interface for producers"
  (send!          [this record] [this topic k v] [this topic k v headers]
    "Produce a record on a topic.
     When using the single arity version, a map
     with the following possible keys is expected:
     `:key`, `:topic`, `:partition`, `:headers`, `:timestamp` and `:value`.
     ")
  (send-cb!       [this record cb] [this topic k v cb] [this topic k v headers cb]
    "Produce a record on a topic with a callback function of 2 arguments:
     - a producer record map as per rm->data,
     - an exception thrown during the processing of a record.
     One of the two arguments will be nil depending on the send result.
     ")
  (flush!         [this]
    "Ensure that produced messages are flushed.")
  (init-transactions! [this])
  (begin-transaction! [this])
  (commit-transaction! [this]))

(defprotocol GenericDriver
  (close!         [this] [this timeout]
    "Close this driver"))

(defn serializer
  "Yield an instance of a serializer from a function of two arguments:
   a topic and the payload to serialize."
  [f]
  (reify
    Serializer
    (close [this])
    (configure [this configs is-key?])
    (serialize [this topic payload]
      (f topic payload))))

(defn edn-serializer
  "Serialize as EDN."
  []
  (serializer
   (fn [_ payload] (some-> payload pr-str .getBytes))))

(defn json-serializer
  "Serialize as JSON through jsonista."
  []
  (serializer
   (fn [_ payload] (some-> payload json/write-value-as-bytes))))

(defn keyword-serializer
  "Serialize keywords to strings, useful for keys."
  []
  (serializer (fn [_ k] (some-> k name .getBytes))))

(defn string-serializer
  "Kafka's own string serializer."
  []
  (StringSerializer.))

(defn deserializer
  "Yield an instance of a deserializer from a function of two arguments:
   a topic and the payload to deserialize."
  [f]
  (reify
    Deserializer
    (close [this])
    (configure [this configs is-key?])
    (deserialize [this topic payload]
      (f topic payload))))

(defn edn-deserializer
  "Deserialize EDN."
  ([]
   (deserializer
    (fn [_ #^"[B" payload]
      (when payload
        (edn/read-string (String. payload "UTF-8"))))))
  ([reader-opts]
   (deserializer
    (fn [_ #^"[B" payload]
      (when payload
        (edn/read-string reader-opts (String. payload "UTF-8")))))))

(defn json-deserializer
  "Deserialize JSON."
  []
  (let [mapper (json/object-mapper {:encode-key-fn name
                                    :decode-key-fn keyword})]
    (deserializer
     (fn [_ #^"[B" payload]
       (when payload
         (json/read-value payload mapper))))))

(defn keyword-deserializer
  "Deserialize a string and then keywordize it."
  []
  (deserializer (fn [_ #^"[B" k] (when k (keyword (String. k "UTF-8"))))))

(defn string-deserializer
  "Kafka's own string deserializer"
  []
  (StringDeserializer.))

(def deserializers
  {:edn     edn-deserializer
   :keyword keyword-deserializer
   :string  string-deserializer
   :json    json-deserializer})

(def serializers
  {:edn     edn-serializer
   :keyword keyword-serializer
   :string  string-serializer
   :json    json-serializer})

(defn ^Deserializer ->deserializer
  [x]
  (cond
    (keyword? x) (if-let [f (deserializers x)]
                   (f)
                   (throw (ex-info "unknown deserializer alias" {})))
    (fn? x)      (x)
    :else        x))

(defn ->serializer
  ^Serializer
  [x]
  (cond
    (keyword? x) (if-let [f (serializers x)]
                   (f)
                   (throw (ex-info "unknown serializer alias" {})))
    (fn? x)     (x)
    :else        x))

(defn opts->props
  "Kakfa configs are now maps of strings to strings. Morphs an arbitrary
  clojure map into this representation.  Make sure we don't pass
  options that are meant for the driver to concrete Consumers/Producers"
  ^Map
  [opts]
  (into {}
        (comp
         (filter (fn [[k _]] (not (qualified-keyword? k))))
         (map (fn [[k v]] [(name k) (str v)])))
        opts))

(defn rebalance-listener
  "Wrap a callback to yield an instance of a Kafka ConsumerRebalanceListener.
   The callback is a function of one argument, a map containing the following
   keys: :event, :topic and :partition. :event will be either :assigned or
   :revoked."
  ^ConsumerRebalanceListener [callback]
  (if (instance? ConsumerRebalanceListener callback)
    callback
    (let [->part  (fn [^TopicPartition p] {:topic (.topic p) :partition (.partition p)})
          ->parts (fn [ps m] (assoc m :partitions (mapv ->part ps)))]
      (reify
        ConsumerRebalanceListener
        (onPartitionsAssigned [this partitions]
          (callback
           (->parts partitions {:event :assigned})))
        (onPartitionsRevoked [this partitions]
          (callback
           (->parts partitions {:event :revoked})))))))

(defn ->topic-partition
  "Yield a TopicPartition from a clojure map."
  ^TopicPartition [{:keys [topic partition]}]
  (TopicPartition. (name topic) (int partition)))

(defn ->offset-metadata
  "Yield a OffsetAndMetadata from a clojure map."
  [{:keys [offset metadata]}]
  (OffsetAndMetadata. offset metadata))

(defn node->data
  "Yield a clojure representation of a node."
  [^Node n]
  {:host (.host n)
   :id   (.id n)
   :port (long (.port n))})

(defn partition-info->data
  "Yield a clojure representation of a partition-info."
  [^PartitionInfo pi]
  {:isr       (mapv node->data (.inSyncReplicas pi))
   :leader    (node->data (.leader pi))
   :partition (long (.partition pi))
   :replicas  (mapv node->data (.replicas pi))
   :topic     (.topic pi)})

(defn topic-partition->data
  "Yield a clojure representation of a topic-partition"
  [^TopicPartition tp]
  {:partition (.partition tp)
   :topic     (.topic tp)})

(def record-xform
  "A transducer to explode grouped records into individual
   entities.

   When sucessful, the output of kinsky.client/poll! takes the
   form:

       {:partitions   [[\"t\" 0] [\"t\" 1]]
        :topics       #{\"t\"}
        :count        2
        :by-partition {[\"t\" 0] [{:key       \"k0\"
                                   :offset    1
                                   :partition 0
                                   :topic     \"t\"
                                   :value     \"v0\"}]
                       [\"t\" 1] [{:key       \"k1\"
                                   :offset    1
                                   :partition 1
                                   :topic     \"t\"
                                   :value     \"v1\"}]}
        :by-topic      {\"t\" [{:key       \"k0\"
                                :offset    1
                                :partition 0
                                :topic     \"t\"
                                :value     \"v0\"}
                               {:key       \"k1\"
                                :offset    1
                                :partition 1
                                :topic     \"t\"
                                :value     \"v1\"}]}}

   To make working with the output channel easier, this
   transducer morphs these messages into a list of
   distinct records:

       ({:key \"k0\" :offset 1 :partition 0 :topic \"t\" :value \"v0\"}
        {:key \"k1\" :offset 1 :partition 1 :topic \"t\" :value \"v1\"}
        ...)
  "
  (comp (map :by-partition) (mapcat vals) cat))

(def value-meta-xform
  "A transducer which acts like `record-xform` but directly yields values,
   with record metadata stored in the map metadata"
  (comp record-xform
        (map (fn [{:keys [value] :as record}]
               (with-meta value
                 (select-keys record [:key :offset :partition :topic]))))))

(def value-xform
  "A transducer which acts like `record-xform` but directly yields values,
   losing metadata"
  (comp record-xform (map :value)))

(defn headers->map [^Headers headers]
  "Build a clojure map out of Kafka Headers from the returned RecordHeaders object. As the Kafka Headers can have
  duplicates, the map returned merges any such cases: {:h1 [\"123\" \"456\"] :h2 [\"Hello\"]}"
  (apply merge-with into {} (map (fn [^Header header]
                                   {(keyword (.key header))
                                    [(slurp (.value header) :encoding "UTF-8")]})
                                 headers)))

(defn cr->data
  "Yield a clojure representation of a consumer record"
  [^ConsumerRecord cr]
  {:key       (.key cr)
   :offset    (.offset cr)
   :partition (.partition cr)
   :timestamp (.timestamp cr)
   :topic     (.topic cr)
   :value     (.value cr)
   :headers   (headers->map (.headers cr))})

(defn crs->eduction
  "Returns consumer records as clojure.lang.Eduction to be used in
  potential reducible context"
  [^ConsumerRecords crs]
  (eduction (map cr->data) crs))

(defn crs-for-topic->eduction
  "Returns an Eduction for records by topics"
  [^ConsumerRecords crs ^String topic]
  (eduction (map cr->data) (.records crs topic)))

(defn crs-for-topic+partition->eduction
  "Returns an Eduction for records for TopicPartition"
  [^ConsumerRecords crs topic partition]
  (let [tp (TopicPartition. topic
                            (int partition))]
    (eduction (map cr->data) (.records crs tp))))

(defn consumer-records->data
  "Yield the default Clojure representation of topic"
  [^ConsumerRecords crs]
  (let [ps (.partitions crs)
        ts   (into #{}
                   (map (fn [^TopicPartition p]
                          (.topic p)))
                   ps)
        edc (crs->eduction crs)]
    {:partitions   ps
     :topics       ts
     :count        (.count crs)
     :by-topic     (group-by :topic edc)
     :by-partition (group-by (juxt :topic :partition) edc)}))

(defn ->topics
  "Yield a valid topic object for subscription given a string, keyword,
  regex pattern or collection of strings or keywords."
  ^Collection
  [topics]
  (assert (or (string? topics)
              (keyword? topics)
              (instance? Pattern topics)
              (and (instance? Collection topics)
                   (every? (some-fn string? keyword?) topics)))
          (str "topics argument must be a string, keyword, regex pattern or "
               "collection of strings or keywords, received " topics))
  (cond
    (keyword? topics)             [(name topics)]
    (string? topics)              [topics]
    (instance? Collection topics) (mapv name topics)
    (instance? Pattern topics)    topics
    :else (throw (ex-info "topics argument is invalid" {:topics topics}))))

(defn consumer->driver
  "Given a KafkaConsumer and an optional callback to call when stopping,
   yield a consumer driver.

   The consumer driver implements the following protocols:

   - [ConsumerDriver](#var-ConsumerDriver)
   - [MetadataDriver](#var-MetadataDriver)
   - `clojure.lang.IDeref`: `deref` to access underlying
     [KafkaConsumer](http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/producer/KafkaConsumer.html)
     instance.

  The consumer-driver can also take options:

  * `consumer-decoder-fn`: a function that will potentially transform
  the ConsumerRecords returned by kafka. By default it will use
  `consumer-records->data`"
  ([^KafkaConsumer consumer]
   (consumer->driver consumer nil))
  ([^KafkaConsumer consumer
    {::keys [run-fn consumer-decoder-fn]
     :or {consumer-decoder-fn consumer-records->data}}]
   (reify
     ConsumerDriver
     (poll! [this timeout]
       (consumer-decoder-fn (.poll consumer (java.time.Duration/ofMillis timeout))))
     (stop! [this]
       (stop! this 0))
     (stop! [this timeout]
       (when (fn? run-fn)
         (run-fn))
       (.wakeup consumer))
     (pause! [this topic-partitions]
       (.pause consumer
               (map ->topic-partition topic-partitions)))
     (resume! [this topic-partitions]
       (.resume consumer
                (map ->topic-partition topic-partitions)))
     (subscribe! [this topics]
       (.subscribe consumer (->topics topics)))
     (subscribe! [this topics listener]
       (.subscribe consumer (->topics topics) (rebalance-listener listener)))
     (unsubscribe! [this]
       (.unsubscribe consumer))
     (wake-up! [this]
       (.wakeup consumer))
     (commit! [this]
       (.commitSync consumer))
     (commit! [this topic-offsets]
       (let [offsets (->> topic-offsets
                          (map (juxt ->topic-partition ->offset-metadata))
                          (reduce merge {}))]
         (.commitSync consumer ^Map offsets)))
     (seek! [this topic-partition offset]
       (.seek consumer
              (->topic-partition topic-partition)
              (long offset)))
     (position! [this topic-partition]
       (.position consumer (->topic-partition topic-partition)))
     (subscription [this]
       (.subscription consumer))
     GenericDriver
     (close! [this]
       (.close consumer))
     MetadataDriver
     (partitions-for [this topic]
       (mapv partition-info->data (.partitionsFor consumer topic)))
     clojure.lang.IDeref
     (deref [this]
       consumer))))

(defn safe-poll!
  "Implementation of poll which disregards wake-up exceptions"
  [consumer timeout]
  (try
    (poll! consumer timeout)
    (catch WakeupException _)))

(defn ->header
  "Build a Kafka header from a key and a value"
  (^Header
   [[k v]]
   (->header k v))
  (^Header
   [k v]
   (reify
     Header
     (key [_] (if (keyword? k) (name k) (str k)))
     (value [_] (byte-array (map byte (str v))))
     (toString [_] (str "RecordHeader(key = " k ", value = " v ")")))))

(defn ->headers [headers]
  "Build Kafka headers from a clojure map."
  (map ->header headers))

(defn ->record
  "Build a ProducerRecord from a clojure map.
   Leave ProducerRecord instances untouched."
  ^ProducerRecord
  [payload]
  (if (instance? ProducerRecord payload)
    payload
    (let [{:keys [partition key value headers timestamp]} payload
          topic (some-> payload :topic name)]
      (when (nil? topic)
        (throw (ex-info "Need a topic to send to" {:payload payload})))

      (when (and headers (nil? timestamp) (nil? partition))
        (throw (ex-info "Need a partition and/or timestamp if you want to add headers" {:payload payload})))

      (ProducerRecord. (str topic)
                       (when partition (int partition))
                       (when timestamp (long timestamp))
                       key value
                       (->headers headers)))))

(defn rm->data
  "Yield a clojure representation of a producer RecordMetadata.
  The map returned is in the form:
  {:topic     \"topic-name\"
   :partition 0 ;; nil if unknown
   :offset    1234567890
   :timestamp 9876543210}"
  [^RecordMetadata rm]
  {:topic     (.topic rm)
   :partition (let [partition (.partition rm)]
                (when (not= partition RecordMetadata/UNKNOWN_PARTITION)
                  partition))
   :offset    (.offset rm)
   :timestamp (.timestamp rm)})

(defn ->callback
  "Return a producer Callback instance given a function taking 2 arguments,
    - a producer record map as per rm->data,
    - an exception thrown during the processing of a record.
   One of the two argument will be nil depending on the send result."
  ^Callback
  [f]
  (when f
    (reify
      Callback
      (onCompletion [_ record-metadata exception]
        (f (some-> record-metadata rm->data) exception)))))

(defn producer->driver
  "Yield a driver from a Kafka Producer.
   The producer driver implements the following protocols:

   - [ProducerDriver](#var-ProducerDriver)
   - [MetadataDriver](#var-MetadataDriver)
   - `clojure.lang.IDeref`: `deref` to access underlying
     [KafkaProducer](http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
     instance."
  [^KafkaProducer producer]
  (reify
    GenericDriver
    (close! [this]
      (.close producer))
    (close! [this timeout]
      (if (nil? timeout)
        (.close producer)
        (.close producer (long timeout) TimeUnit/MILLISECONDS)))
    ProducerDriver
    (send! [this record]
      (.send producer (->record record)))
    (send! [this topic k v]
      (.send producer (->record {:key k :value v :topic topic})))
    (send! [this topic k v headers]
      (.send producer (->record {:key k :value v :topic topic :headers headers})))
    (send-cb! [this record cb]
      (.send producer (->record record) (->callback cb)))
    (send-cb! [this topic k v cb]
      (.send producer
             (->record {:key k :value v :topic topic})
             (->callback cb)))
    (send-cb! [this topic k v headers cb]
      (.send producer
             (->record {:key k :value v :topic topic :headers headers})
             (->callback cb)))
    (flush! [this]
      (.flush producer))
    (init-transactions! [this]
      (.initTransactions producer))
    (begin-transaction! [this]
      (.beginTransaction producer))
    (commit-transaction! [this]
      (.commitTransaction producer))
    MetadataDriver
    (partitions-for [this topic]
      (mapv partition-info->data (.partitionsFor producer topic)))
    clojure.lang.IDeref
    (deref [this]
      producer)))

(defn producer
  "Create a producer from a configuration and optional serializers.
   If a single serializer is provided, it will be used for both keys
   and values. If none are provided, the configuration is expected to
   hold serializer class names."
  ([config]
   (producer->driver (KafkaProducer. (opts->props config))))
  ([config serializer]
   (producer->driver (KafkaProducer. (opts->props config)
                                     (->serializer serializer)
                                     (->serializer serializer))))
  ([config kserializer vserializer]
   (producer->driver (KafkaProducer. (opts->props config)
                                     (->serializer kserializer)
                                     (->serializer vserializer)))))

(defn consumer
  "Create a consumer from a configuration and optional deserializers.
   If a callback is given, call it when stopping the consumer.
   If deserializers are provided, use them otherwise expect deserializer
   class name in the config map."
  ([config]
   (consumer->driver (KafkaConsumer. (opts->props config))
                     config))
  ([config callback]
   (consumer->driver (KafkaConsumer. (opts->props config))
                     (assoc config ::run-fn callback)))
  ([config kdeserializer vdeserializer]
   (consumer->driver (KafkaConsumer. (opts->props config)
                                     (->deserializer kdeserializer)
                                     (->deserializer vdeserializer))
                     config))
  ([config callback kdeserializer vdeserializer]
   (consumer->driver (KafkaConsumer. (opts->props config)
                                     (->deserializer kdeserializer)
                                     (->deserializer vdeserializer))
                     (assoc config ::run-fn callback))))
