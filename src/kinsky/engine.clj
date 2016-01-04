(ns kinsky.engine
  (:require [clojure.edn           :as edn]
            [cheshire.core         :as json]
            [clojure.tools.logging :refer [warn]])
  (:import java.util.Properties
           org.apache.kafka.clients.consumer.KafkaConsumer
           org.apache.kafka.clients.consumer.ConsumerRebalanceListener
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.producer.ProducerRecord
           org.apache.kafka.common.TopicPartition
           org.apache.kafka.common.serialization.Serializer
           org.apache.kafka.common.serialization.Deserializer
           org.apache.kafka.common.errors.WakeupException))

(defprotocol ConsumerDriver
  ""
  (poll!          [this timeout])
  (stop!          [this] [this timeout])
  (pause!         [this] [this topic-partitions])
  (resume!        [this topic-partitions])
  (partitions-for [this topic])
  (subscribe!     [this topics] [this topics listener]))

;; Common
(defn serializer
  [f]
  (reify
    Serializer
    (close [this])
    (configure [this configs is-key?])
    (serialize [this topic payload]
      (f topic payload))))

(defn edn-serializer
  []
  (serializer
   (fn [_ payload] (-> payload pr-str .getBytes))))

(defn json-serializer
  []
  (serializer
   (fn [_ payload] (-> payload json/generate-string .getBytes))))

(defn keyword-serializer
  []
  (serializer (fn [_ k] (-> k name .getBytes))))

(defn deserializer
  [f]
  (reify
    Deserializer
    (close [this])
    (configure [this configs is-key?])
    (deserialize [this topic payload]
      (f topic payload))))

(defn edn-deserializer
  []
  (deserializer
   (fn [_ payload]
     (edn/read-string (String. payload "UTF-8")))))

(defn json-deserializer
  []
  (deserializer
   (fn [_ payload]
     (json/parse-string (String. payload "UTF-8") true))))

(defn keyword-deserializer
  []
  (deserializer (fn [_ k] (keyword (String. k "UTF-8")))))

;; Client (producer)

(defn opts->props
  [opts]
  (into {} (for [[k v] opts] [(name k) (str v)])))

(defn kvclient
  [opts]
  (KafkaProducer. (opts->props opts)))

(defn setk! [p k v]
  (.send p (ProducerRecord. "kvstore" (name k) (pr-str v))))

(comment
  (kvclient {:bootstrap.servers "localhost:9092"
             :key.serializer    "org.apache.kafka.common.serialization.StringSerializer"
             :value.serializer  "org.apache.kafka.common.serialization.StringSerializer"})

  (setk! k :bar "hello"))

;; Server (consumer)

(defn format-partitions
  [partitions default]
  (assoc default :partitions (into [] (for [p partitions]
                                        {:topic     (.topic p)
                                         :partition (.partition p)}))))

(defn rebalance-listener
  [callback]
  (reify
    ConsumerRebalanceListener
    (onPartitionsAssigned [this partitions]
      (callback
       (format-partitions partitions {:event :assigned})))
    (onPartitionsRevoked [this partitions]
      (callback
       (format-partitions partitions {:event :revoked})))))

(defn ->TopicPartition
  [{:keys [topic partition]}]
  (TopicPartition. (name topic) (int partition)))

(defn consumer-driver
  [^KafkaConsumer consumer run-signal]
  (reify
    ConsumerDriver
    (poll! [this timeout]
      (.poll consumer timeout))
    (stop! [this timeout]
      (run-signal)
      (.wakeup consumer))
    (pause! [this topic-partitions]
      (.pause consumer
              (into-array TopicPartition (map ->TopicPartition
                                              topic-partitions))))
    (resume! [this topic-partitions]
      (.resume consumer
               (into-array TopicPartition (map ->TopicPartition
                                               topic-partitions))))
    (partitions-for [this topic]
      (.partitionsFor consumer topic))
    (subscribe! [this topics]
      (assert (or (string? topics)
                  (instance? java.util.regex.Pattern topics)
                  (and (instance? java.util.List topics)
                       (every? string? topics)))
              "topic argument must be a string, regex pattern or
               collection of strings.")
      (.subscribe consumer topics))
    (subscribe! [this topics listener]
      (assert (or (string? topics)
                  (instance? java.util.regex.Pattern topics)
                  (and (instance? java.util.List topics)
                       (every? string? topics)))
              "topic argument must be a string, regex pattern or
               collection of strings.")
      (.subscribe consumer topics (rebalance-listener listener)))))

(defn kvlistener
  [{:keys [event partitions]}]
  (warn "the following partitions were" (name event) (pr-str partitions)))

(defn kvserver
  [opts topics {:keys [deserializer serializer listener]} sink!]
  (let [run?     (atom true)
        consumer (KafkaConsumer. (opts->props opts) deserializer serializer)
        driver   (consumer-driver consumer (fn [] (reset! run? false)))]
    (future
      (try
        (while @run?
          (sink! (poll! consumer 100)))
        (catch WakeupException e
          (when @run?
            (throw e)))
        (finally
          (.close consumer))))
    driver))
