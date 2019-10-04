(defproject spootnik/kinsky "0.1.24-SNAPSHOT"
  :description "Kafka clojure client library"
  :plugins [[lein-codox "0.9.1"]
            [lein-ancient "0.6.15"]]
  :url "https://github.com/pyr/kinsky"
  :license {:name "MIT License"
            :url  "https://github.com/pyr/kinsky/tree/master/LICENSE"}
  :codox {:source-uri "https://github.com/pyr/kinsky/blob/{version}/{filepath}#L{line}"
          :metadata   {:doc/format :markdown}}
  :global-vars {*warn-on-reflection* true}
  :dependencies [[org.clojure/clojure            "1.10.0"]
                 [org.clojure/core.async         "0.4.490"]
                 [org.apache.kafka/kafka-clients "2.3.0"]
                 [cheshire                       "5.8.1"]]
  :test-selectors {:default     (complement :integration)
                   :integration :integration
                   :all         (constantly true)}
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-nop "1.7.25"]
                                  ;; for kafka embedded
                                  [org.apache.kafka/kafka_2.12 "2.3.0"]
                                  [org.apache.zookeeper/zookeeper "3.4.14"
                                   :exclusions [io.netty/netty
                                                jline
                                                org.apache.yetus/audience-annotations
                                                org.slf4j/slf4j-log4j12
                                                log4j]]]}})
