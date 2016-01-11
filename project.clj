(defproject spootnik/kinsky "0.1.8"
  :description "Kafka clojure client library"
  :plugins [[lein-codox "0.9.1"]]
  :url "https://github.com/pyr/kinsky"
  :license {:name "MIT License"
            :url  "https://github.com/pyr/kinsky/tree/master/LICENSE"}
  :codox {:source-uri "https://github.com/pyr/kinsky/blob/{version}/{filepath}#L{line}"
          :metadata   {:doc/format :markdown}}
  :dependencies [[org.clojure/clojure            "1.7.0"]
                 [org.clojure/core.async         "0.2.374"]
                 [org.apache.kafka/kafka-clients "0.9.0.0"]
                 [cheshire                       "5.5.0"]])
