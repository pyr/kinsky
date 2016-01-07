(defproject spootnik/kinsky "0.1.1"
  :description "Kafka processing library"
  :plugins [[lein-codox "0.9.1"]]
  :codox {:source-uri "https://github.com/pyr/kinsky/blob/{version}/{filepath}#L{line}"
          :metadata   {:doc/format :markdown}}
  :dependencies [[org.clojure/clojure            "1.7.0"]
                 [org.clojure/tools.logging      "0.3.1"]
                 [org.clojure/core.async         "0.2.374"]
                 [org.apache.kafka/kafka-clients "0.9.0.0"]
                 [cheshire                       "5.5.0"]
                 [spootnik/uncaught              "0.5.3"]])
