(ns kinsky.client-test
  (:require [clojure.test  :refer :all]
            [kinsky.client :as client]))

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
                         (.getBytes "{\"a\": \"b\", \"c\": \"d\"}")))))

  (testing "json deserializer, log and continue on error"
    (is (= {:json-deserializer-error "Bad Message Detected" :message "F00"}
           (.deserialize (client/json-deserializer-lac) ""
                         (.getBytes "F00"))))))

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
    (let [producer (client/producer {:bootstrap.servers "localhost:9092"} :string :string)]
      (client/flush! producer))))

(deftest ^:integration consumer
  (testing "stop"
    (let [consumer (client/consumer {:bootstrap.servers "localhost:9092"} :string :string)]
      (client/stop! consumer))))

(deftest ^:integration topics
  (testing "collection of keywords"
    (let [consumer (client/consumer {:bootstrap.servers "localhost:9092"} :string :string)]
      (client/subscribe! consumer [:x :y :z]))))
