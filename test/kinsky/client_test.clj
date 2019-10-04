(ns kinsky.client-test
  (:require [clojure.test  :refer :all :as t]
            [clojure.pprint :as pp]
            [kinsky.client :as client]
            [kinsky.embedded :as e]))

(def host "localhost")
(def kafka-port 9093)
(def zk-port 2183)
(def bootstrap-servers (format "%s:%s" host kafka-port))

(t/use-fixtures
  :once (fn [f]
          (let [z-dir (e/create-tmp-dir "zookeeper-data-dir")
                k-dir (e/create-tmp-dir "kafka-log-dir")]
            (try
              (with-open [k (e/start-embedded-kafka
                             {::e/host host
                              ::e/kafka-port kafka-port
                              ::e/zk-port zk-port
                              ::e/zookeeper-data-dir (str z-dir)
                              ::e/kafka-log-dir (str k-dir)
                              ::e/broker-config {"auto.create.topics.enable" "true"}})]
                (f))
              (catch Throwable t
                (throw t))
              (finally
                (e/delete-dir z-dir)
                (e/delete-dir k-dir))))))

(deftest serializer
  (testing "string serializer"
    (is (= "foo"
           (String.
            (.serialize (client/string-serializer) "" "foo")))))

  (testing "keyword serializer"
    (is (= "foo"
           (String.
            (.serialize (client/keyword-serializer) "" :foo)))))

  (testing "edn serializer"
    (is (= "{:a :b, :c :d}"
           (String.
            (.serialize (client/edn-serializer) "" {:a :b :c :d})))))

  (testing "json serializer"
    (is (= "[0,1,2]"
           (String.
            (.serialize (client/json-serializer) "" [0 1 2]))))))

(deftest deserializer
  (testing "string deserializer"
    (is (= "foo" (.deserialize (client/string-deserializer) ""
                               (.getBytes "foo")))))

  (testing "keyword deserializer"
    (is (= :foo (.deserialize (client/keyword-deserializer) ""
                              (.getBytes "foo")))))

  (testing "edn deserializer"
    (is (= {:a :b :c :d}
           (.deserialize (client/edn-deserializer) ""
                         (.getBytes "{:a :b, :c :d}")))))

  (testing "json deserializer"
    (is (= {:a "b" :c "d"}
           (.deserialize (client/json-deserializer) ""
                         (.getBytes "{\"a\": \"b\", \"c\": \"d\"}"))))))

(deftest config-props
  (testing "valid configuration properties"
    (is (= {"foo.bar" "0"}
           (client/opts->props {:foo.bar 0})))))

(deftest rebalance-listener
  (testing "idempotency"
    (let [sink (client/rebalance-listener (fn [& _]))]
      (is (= sink (client/rebalance-listener sink)))))

  (testing "expected events"
    (let [db   (atom nil)
          tp1  (client/->topic-partition {:topic "t" :partition 1})
          tp2  (client/->topic-partition {:topic "t" :partition 2})
          tp3  (client/->topic-partition {:topic "t" :partition 3})
          tp4  (client/->topic-partition {:topic "t" :partition 4})
          sink (client/rebalance-listener (fn [x] (swap! db conj x)))]
      (.onPartitionsAssigned sink [tp1 tp2])
      (.onPartitionsRevoked sink [tp3 tp4])
      (is (= (vec @db)
             [{:event :revoked  :partitions [{:topic "t" :partition 3}
                                             {:topic "t" :partition 4}]}
              {:event :assigned :partitions [{:topic "t" :partition 1}
                                             {:topic "t" :partition 2}]}])))))

(deftest topic-partition
  (testing "topic-partition"
    (let [part (client/->topic-partition {:topic "t" :partition 1})]
      (is (= (int 1) (.partition part)))
      (is (= "t" (.topic part))))))

(deftest ^:integration producer
  (testing "flush"
    (let [producer (client/producer {:bootstrap.servers bootstrap-servers}
                                    :string :string)]
      (client/flush! producer))))

(deftest ^:integration consumer
  (testing "stop"
    (let [consumer (client/consumer {:bootstrap.servers bootstrap-servers}
                                    :string :string)]
      (client/stop! consumer))))

(deftest topics
  (testing "collection of keywords"
    (let [consumer (client/consumer {:bootstrap.servers bootstrap-servers
                                     :group.id "consumer-group-id"}
                                    :string :string)]
      (client/subscribe! consumer [:x :y :z]))))

(deftest roundtrip
  (let [msgs {:account-a {:action :login}
              :account-b {:action :logout}
              :account-c {:action :register}}
        p (client/producer {:bootstrap.servers bootstrap-servers}
                           (client/keyword-serializer)
                           (client/edn-serializer))
        base-consumer-conf {:bootstrap.servers bootstrap-servers
                            "enable.auto.commit" "false"
                            "auto.offset.reset" "earliest"
                            "isolation.level" "read_committed"}
        d (client/keyword-deserializer)
        s (client/edn-deserializer)
        setup! (fn [c t]
                 (client/subscribe! c t)
                 (doseq [[k v] msgs]
                   @(client/send! p t k v)))]
    (testing "msg roundtrip"
      (let [t "account"
            c (client/consumer (assoc base-consumer-conf
                                      :group.id "consumer-group-id-1")
                               d s)]
        (setup! c t)
        (is (= (->> (client/poll! c 5000)
                    :by-topic
                    vals
                    (apply concat)
                    (map :value))
               (vals msgs)))))

    (testing "msg roundtrip crs->eduction simple"
      (let [t "account2"
            c (client/consumer (assoc base-consumer-conf
                                      :group.id "consumer-group-id-2"
                                      :kinsky.client/consumer-decoder-fn
                                      #(into [] (client/crs->eduction %)))
                               d s)]
        (setup! c t)
        (is (= (->> (client/poll! c 5000)
                    (map :value))
               (vals msgs)))))

    (testing "msg roundtrip crs-for-topic->eduction"
      (let [t "account3"
            c (client/consumer (assoc base-consumer-conf
                                      :group.id "consumer-group-id-3"
                                      :kinsky.client/consumer-decoder-fn
                                      #(into [] (client/crs-for-topic->eduction % t)))
                               d s)]
        (setup! c t)
        (is (= (->> (client/poll! c 5000)
                    (map :value))
               (vals msgs)))))

    (testing "msg roundtrip crs-for-topic->eduction"
      (let [t "account4"
            c (client/consumer (assoc base-consumer-conf
                                      :group.id "consumer-group-id-4"
                                      :kinsky.client/consumer-decoder-fn
                                      #(into [] (client/crs-for-topic+partition->eduction % t 0)))
                               d s)]
        (setup! c t)
        (is (= (->> (client/poll! c 5000)
                    (map :value))
               (vals msgs)))))

    (testing "msg roundtrip crs->eduction sequence"
      (let [t "account5"
            c (client/consumer (assoc base-consumer-conf
                                      :group.id "consumer-group-id-5"
                                      :kinsky.client/consumer-decoder-fn
                                      #(sequence (client/crs-for-topic+partition->eduction % t 0)))
                               d s)]
        (setup! c t)
        (is (= (->> (client/poll! c 5000)
                    (map :value))
               (vals msgs)))))))

(deftest headers
  (testing "Kafka Headers, making sure we have implemented the interfaces required by the Producer and Consumer
            to process the key value pairs. NOTE: all header values returned will be converted to string values.
            The first test header has duplicate keys :h1 and \"h1\" as the implementation converts all keywords
            to strings before storing them in the Headers ArrayList and converts them back to keywords when converting
            them back to a Clojure map. Duplicate key values are a valid scenario in Kafka for some wierd reason."
    (let [headers (client/->headers {:h1 1 :h2 2.0 "h1" "foo"})
          producer (org.apache.kafka.clients.producer.ProducerRecord. "test" (int 0) "key" "value" headers)
          r-headers (.headers producer)]
      (is (instance? org.apache.kafka.common.header.Header (client/->header :h "foo")))
      (is (= "RecordHeader(key = :h1, value = Foo)" (.toString (client/->header :h1 "Foo"))))
      (is (contains? (supers (class headers)) java.lang.Iterable))
      (is (instance? org.apache.kafka.common.header.Headers (.headers producer)))
      (is (= {:h1 ["1" "foo"] :h2 ["2.0"]} (client/headers->map r-headers))))))

(deftest producer-record
  (testing "The 6 different ProducerRecord combinations and one with headers and not partition or timestamp"
    (is (= "ProducerRecord(topic=test, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=null, value=Value, timestamp=null)"
           (.toString (client/->record {:topic "test" :value "Value"}))))
    (is (= "ProducerRecord(topic=test, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=Key, value=Value, timestamp=null)"
           (.toString (client/->record {:topic "test" :key "Key" :value "Value"}))))
    (is (= "ProducerRecord(topic=test, partition=0, headers=RecordHeaders(headers = [], isReadOnly = false), key=Key, value=Value, timestamp=null)"
           (.toString (client/->record {:topic "test" :partition 0 :key "Key" :value "Value"}))))
    (is (= "ProducerRecord(topic=test, partition=1, headers=RecordHeaders(headers = [], isReadOnly = false), key=Key, value=Value, timestamp=65535)"
           (.toString (client/->record {:topic "test" :partition 1 :timestamp 65535 :key "Key" :value "Value"}))))
    (is (= "ProducerRecord(topic=test, partition=2, headers=RecordHeaders(headers = [RecordHeader(key = :h1, value = one)], isReadOnly = false), key=Key, value=Value, timestamp=null)"
           (.toString (client/->record {:topic "test" :partition 2 :key "Key" :value "Value" :headers {:h1 "one"}}))))
    (is (= "ProducerRecord(topic=test, partition=2, headers=RecordHeaders(headers = [RecordHeader(key = :h1, value = one)], isReadOnly = false), key=Key, value=Value, timestamp=1234567890)"
           (.toString (client/->record {:topic "test" :partition 2 :timestamp 1234567890 :key "Key" :value "Value" :headers {:h1 "one"}}))))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Need a partition and/or timestamp if you want to add headers" (.toString (client/->record {:topic "test" :key "Key" :value "Value" :headers {:h1 "one"}}))))))
