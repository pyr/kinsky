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
  (testing "msg roundtrip"
    (let [t "account"
          p (client/producer {:bootstrap.servers bootstrap-servers}
                             (client/keyword-serializer)
                             (client/edn-serializer))
          c (client/consumer {:bootstrap.servers bootstrap-servers
                              :group.id "consumer-group-id"
                              "enable.auto.commit" "false"
                              "auto.offset.reset" "earliest"
                              "isolation.level" "read_committed"}
                             (client/keyword-deserializer)
                             (client/edn-deserializer))
          msgs {:account-a {:action :login}
                :account-b {:action :logout}
                :account-c {:action :register}}]

      (client/subscribe! c t)

      (doseq [[k v] msgs]
        @(client/send! p t k v))

      (is (= (->> (client/poll! c 5000)
                  :by-topic
                  vals
                  (apply concat)
                  (map :value))
           (vals msgs))))))
