(ns kinsky.admin
  "Wrapper around Kafka's `AdminClient`"
  (:require [kinsky.client :refer (GenericDriver opts->props)])
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

(defn kafka-future-wrapper
  "Return a promise representing the result of a `KafkaFuture`
   transformed by the `result-transform` function."
  [^KafkaFuture kafka-future result-transform]
  (let [p   (promise)
        kfn (proxy [KafkaFuture$Function] []
              (apply [result]
                (deliver p (result-transform result))))]
    (.thenApply kafka-future kfn)
    p))

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
          (kafka-future-wrapper #(mapv topic-listing->data %)))))

  (create-topic
    [this
     topic-name
     {:keys [replication-factor
             partitions
             config]
      :or   {partitions         1
             replication-factor 1}}]
    (let [topic (NewTopic. ^String topic-name
                           (int partitions)
                           (short replication-factor))]
      (when config
        (.configs topic (opts->props config)))
      (-> (.createTopics this (list topic))
          .all
          (kafka-future-wrapper #(when (nil? %) true))))))

(defn client
  "Create an AdminClient from a configuration map."
  [config]
  (AdminClient/create (opts->props config)))

;; (def c (client {"bootstrap.servers" "localhost:9092"}))

;; @(list-topics c true)

;; @(create-topic c "my-topic" {:partitions 2 :replication-factor 1 :config {:cleanup.policy "compact"}})
