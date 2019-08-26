(ns kinsky.embedded
  "Based on crux.kafka.embedded"
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s])
  (:import (kafka.server
            KafkaConfig
            KafkaServerStartable)
           (org.apache.zookeeper.server
            ServerCnxnFactory ZooKeeperServer)
           (java.nio.file Files Paths Path FileVisitResult SimpleFileVisitor)
           (java.io Closeable)))

(def default-kafka-broker-config
  {"broker.id" "0"
   "num.io.threads" "5"
   "num.network.threads" "5"
   "log.cleaner.dedupe.buffer.size" "1048577"
   "log.flush.interval.messages"  "1"
   "offsets.topic.num.partitions" "1"
   "offsets.topic.replication.factor" "1"
   "transaction.state.log.num.partitions" "1"
   "transaction.state.log.replication.factor" "1"
   "transaction.state.log.min.isr" "1"
   "auto.create.topics.enable" "false"
   "auto.offset.reset" "earliest"
   "retry.backoff.ms" "500"
   "message.send.max.retries" "5"
   "max.poll.records" "1"})

(defn start-kafka-broker ^KafkaServerStartable [config]
  (doto (KafkaServerStartable. (KafkaConfig. (merge default-kafka-broker-config
                                                    config)))
    (.startup)))

(defn stop-kafka-broker [^KafkaServerStartable broker]
  (some-> broker .shutdown)
  (some-> broker .awaitShutdown))

(def file-deletion-visitor
  (proxy [SimpleFileVisitor] []
    (visitFile [file _]
      (Files/delete file)
      FileVisitResult/CONTINUE)

    (postVisitDirectory [dir _]
      (Files/delete dir)
      FileVisitResult/CONTINUE)))

(defn ^Path create-tmp-dir
  [prefix]
  (Files/createTempDirectory
   (Paths/get (System/getProperty "java.io.tmpdir")
              (make-array String 0))
   (str prefix "-")
   (make-array java.nio.file.attribute.FileAttribute 0)))

(defn delete-dir
  [dir]
  (assert (instance? Path dir))
  (Files/walkFileTree dir
                      file-deletion-visitor))

(defn start-zookeeper
  ^org.apache.zookeeper.server.ServerCnxnFactory
  [data-dir ^long port]
  (let [tick-time 2000
        max-connections 16
        server (ZooKeeperServer. (io/file data-dir)
                                 (io/file data-dir)
                                 tick-time)]
    (doto (ServerCnxnFactory/createFactory port
                                           max-connections)
      (.startup server))))

(defn stop-zookeeper
  [^ServerCnxnFactory server-cnxn-factory]
  (when server-cnxn-factory
    (.shutdown server-cnxn-factory)
    (when-let [server ^ZooKeeperServer (-> (doto (.getDeclaredMethod ServerCnxnFactory
                                                                     "getZooKeeperServer"
                                                                     (make-array Class 0))
                                             (.setAccessible true))
                                           (.invoke server-cnxn-factory
                                                    (object-array 0)))]
      (.shutdown server)
      (some-> (.getZKDatabase server) (.close)))))

(defrecord EmbeddedKafka [zookeeper kafka options]
  Closeable
  (close [_]
    (stop-kafka-broker kafka)
    (stop-zookeeper zookeeper)))

(s/def ::zookeeper-data-dir string?)
(s/def ::zookeeper-port pos-int?)
(s/def ::kafka.log-dir string?)
(s/def ::kafka-port pos-int?)
(s/def ::broker-config (s/map-of string? string?))

(s/def ::options (s/keys :req [::zookeeper-data-dir
                               ::kafka-log-dir]
                         :opt [::zookeeper-port
                               ::kafka-port
                               ::broker-config]))

(defn start-embedded-kafka
  "Starts ZooKeeper and Kafka locally. This can be used to run server in
  a self-contained single node mode. The options zookeeper-data-dir
  and kafka-log-dir are required.

  Returns a EmbeddedKafka component that implements java.io.Closeable,
  which allows ZooKeeper and Kafka to be stopped by calling close."
  ^java.io.Closeable
  [{::keys [host zookeeper-data-dir zookeeper-port
            kafka-log-dir kafka-port
            broker-config broker-id]
    :or {zookeeper-port 2182
         host "localhost"
         kafka-port 9092
         broker-id "0"}
    :as options}]
  (s/assert ::options options)
  (let [zookeeper (start-zookeeper (io/file zookeeper-data-dir)
                                   zookeeper-port)
        kafka (try
                (start-kafka-broker (assoc broker-config
                                           "log.dir" (str (io/file kafka-log-dir))
                                           "port" (str kafka-port)
                                           "host" host
                                           "broker-id" broker-id
                                           "zookeeper.connect" (format "%s:%s"
                                                                       host
                                                                       zookeeper-port)))
                (catch Throwable t
                  (stop-zookeeper zookeeper)
                  (throw t)))]
    (->EmbeddedKafka zookeeper
                     kafka
                     (assoc options
                            :bootstrap-servers (format "%s:%s" host kafka-port)))))
