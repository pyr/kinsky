Kinsky: Clojure Kafka client library
====================================

[![Build Status](https://secure.travis-ci.org/pyr/kinsky.png)](http://travis-ci.org/pyr/kinsky)

Kinsky is a *somewhat* opinionated client library
for [Apache Kafka](http://kakfa.apache.org) in Clojure.

Kinsky provides the following:

- Kakfa 0.10.0.x compatibility
- Adequate data representation of Kafka types.
- Default serializer and deserializer implementations such as
  **JSON**, **EDN** and a **keyword** serializer for keys.
- A `core.async` facade for producers and consumers.
- Documentation

## Usage

```clojure
   [[spootnik/kinsky "0.1.16"]]
```

## Documentation

* [API Documentation](http://pyr.github.io/kinsky)

## Changelog

### 0.1.16

- Stability and bugfix release
- Lots of input from @jmgrov, @scott-abernethy, and @henriklundahl. Thanks!

## Examples

The examples assume the following require forms:

```clojure
(:require [kinsky.client      :as client]
          [kinsky.async       :as async]
          [clojure.core.async :refer [go <! >!]])
```

### Production

```clojure
(let [p (client/producer {:bootstrap.servers "localhost:9092"}
                         (client/keyword-serializer)
                         (client/edn-serializer))]
  (client/send! p "account" :account-a {:action :login}))

```

Async facade:

```clojure
(let [[in out] (async/producer {:bootstrap.servers "localhost:9092"} :keyword :edn)]
   (go
     (>! in {:topic "account" :key :account-a :value {:action :login}})
     (>! in {:topic "account" :key :account-a :value {:action :logout}})))
```

### Consumption

```clojure
(let [c (client/consumer {:bootstrap.servers "localhost:9092"
                          :group.id          "mygroup"}
                         (client/keyword-deserializer)
                         (client/edn-deserializer))]
  (client/subscribe! c "account")
  (client/poll! c 100))

```

Async facade:

```clojure
(let [[out ctl] (consumer {:bootstrap.servers "localhost:9092"
                           :group.id (str (java.util.UUID/randomUUID))}
                          (client/string-deserializer)
                          (client/string-deserializer))
      topic     "tests"]
						  
  (a/go-loop []
    (when-let [record (a/<! out)]
      (println (pr-str record))
      (recur)))
  (a/put! ctl {:op :partitions-for :topic topic})
  (a/put! ctl {:op :subscribe :topic topic})
  (a/put! ctl {:op :commit})
  (a/put! ctl {:op :pause :topic-partitions [{:topic topic :partition 0}
                                             {:topic topic :partition 1}
                                             {:topic topic :partition 2}
                                             {:topic topic :partition 3}]})
  (a/put! ctl {:op :resume :topic-partitions [{:topic topic :partition 0}
                                              {:topic topic :partition 1}
                                              {:topic topic :partition 2}
                                              {:topic topic :partition 3}]})
  (a/put! ctl {:op :stop}))
```

### Examples

#### Fusing two topics

```clojure
  (let [popts    {:bootstrap.servers "localhost:9092"}
        copts    (assoc popts :group.id "consumer-group-id")
        [in ctl] (kinsky.async/consumer copts :string :string)
        [out _]  (kinsky.async/producer popts :string :string)]

    (a/go
      ;; fuse topics
	  (a/>! ctl {:op :subscribe :topic "test1"})
      (let [transit (a/chan 10 (map #(assoc % :topic "test2")))]
        (a/pipe in transit)
        (a/pipe transit out))))
```
