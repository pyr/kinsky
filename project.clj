(defproject spootnik/kinsky "0.1.26-SNAPSHOT"
  :description "Kafka clojure client library"
  :plugins [[lein-ancient "0.6.15"]]
  :url "https://github.com/pyr/kinsky"
  :license {:name "MIT License"
            :url  "https://github.com/pyr/kinsky/tree/master/LICENSE"}
  :global-vars {*warn-on-reflection* true}
  :deploy-repositories [["snapshots" :clojars] ["releases" :clojars]]
  :dependencies [[org.clojure/clojure            "1.10.1"]
                 [org.apache.kafka/kafka-clients "2.7.2"]
                 [metosin/jsonista               "0.2.5"]]
  :test-selectors {:default     (complement :integration)
                   :integration :integration
                   :all         (constantly true)}
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-nop "1.7.30"]
                                  [org.slf4j/slf4j-api "1.7.30"]
                                  ;; for kafka embedded
                                  [org.apache.kafka/kafka_2.12 "2.7.2"]
                                  [org.apache.zookeeper/zookeeper "3.5.6"
                                   :exclusions [io.netty/netty
                                                jline
                                                org.apache.yetus/audience-annotations
                                                org.slf4j/slf4j-log4j12
                                                log4j]]]}})
