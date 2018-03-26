(ns kinsky.admin
  "Wrapper around Kafka's `AdminClient`"
  (:require [kinsky.client :refer (GenericDriver opts->props)]
            [clojure.core.async :as a])
  (:import (org.apache.kafka.clients.admin AdminClient ListTopicsOptions
                                           TopicListing NewTopic)
           java.util.concurrent.TimeUnit
           org.apache.kafka.common.KafkaFuture
           org.apache.kafka.common.KafkaFuture$Function
           clojure.lang.IDeref))

(defprotocol AdminClientDriver
  "Driver protocol for admin clients"
  (close!         [this] [this timeout]
    "Close this driver")
  (list-topics  [this list-internal?]
    "List all available topics. When `list-internal?` is `true` the list of
     internal topics (`__consumer_offsets`) is also returned.")
  (create-topic [this topic-name topic-options]))

(defprotocol KafkaFutureWrapper
  "Small wrapper for `KafkaFuture` instances."
  (to-chan [this] [this channel]
    "Returns a `core.async` channel to which the result is pushed. When
     `channel` is present we use this channel to push data to."))

(defn kafka-future->wrapper
  ;; TODO better docs, test core.async integration
  "Return a wrapper around a `KafkaFuture`. When `mapper` is present, the
   function is applied to the result before being returned or pushed to a
   channel. The wrapper also implements the `IDeref` interface for
   easy synchronous consumption of the result."
  ([^KafkaFuture kafka-future]
   (kafka-future->wrapper kafka-future identity))
  ([^KafkaFuture kafka-future map-result]
   (reify
     KafkaFutureWrapper
     (to-chan [this]
       (let [ch  (a/chan)
             kfn (proxy [KafkaFuture$Function] []
                   (apply [result]
                     (a/put! ch (map-result result))))]
         (.thenApply kafka-future kfn)))
     (to-chan [this ch]
       (let [kfn (proxy [KafkaFuture$Function] []
                   (apply [result]
                     (a/put! ch (map-result result))))]
         (.thenApply kafka-future kfn)))

     IDeref
     (deref [this]
       (-> (.get kafka-future)
           map-result)))))

(defn topic-listing->data
  "Convert a `TopicListing` instance to a data map."
  [^TopicListing listing]
  {:name      (.name listing)
   :internal? (.isInternal listing)})

(extend-type AdminClient
  AdminClientDriver

  (close! [this]
    (.close this))
  (close! [this timeout]
    (.close this (long timeout) TimeUnit/MILLISECONDS))

  (list-topics [this list-internal?]
    (let [opts (doto (ListTopicsOptions.)
                 (.listInternal list-internal?))]
      (-> (.listTopics this opts)
          .listings
          (kafka-future->wrapper #(map topic-listing->data %)))))

  (create-topic
    [this
     topic-name
     {:keys [replication-factor
             partitions
             config]
      :or   {partitions         1
             replication-factor 1}}]
    (let [topic (NewTopic. topic-name partitions replication-factor)]
      (when config
        (.configs topic (opts->props config)))
      (-> (.createTopics this (list topic))
          .all
          (kafka-future->wrapper #(when (nil? %) true))))))

(defn client
  "Create an AdminClient from a configuration map."
  [config]
  (AdminClient/create (opts->props config)))

;; (def c (client {"bootstrap.servers" "localhost:9092"}))

;; @(list-topics c true)

;; @(create-topic c "my-topic" {:partitions 2 :replication-factor 1 :config {:cleanup.policy "compact"}})
