Kinsky: Clojure Kafka client library
====================================

Kinsky is a *somewhat* opiniated client library
for [Apache Kafka](http://kakfa.apache.org) in Clojure.

Kinsky provides the following:

- Adequate data representation of Kafka types.
- Default serializer and deserializer implementations such as
  **JSON**, **EDN** and a **keyword** serializer for keys.
- A `core.async` facade for producers and consumers.
- Documentation

## Usage

```clojure
   [[spootnik/kinsky "0.1.3"]]
```

## Documentation

* [API Documentation](http://pyr.github.io/kinsky)

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
(let [[producer records ctl] (async/producer {:bootstrap.servers "localhost:9092"}
                                             (client/keyword-serializer)
                                             (client/edn-serializer))]
   (go
     (>! records {:topic "account" :key :account-a :value {:action :login}})
     (>! records {:topic "account" :key :account-a :value {:action :logout}})))
```

### Consumption

```clojure
(let [c (client/consumer {:bootstrap.servers "localhost:9092"
                          :group.id          "mygroup"}
                         (client/keyword-deserializer)
                         (client/edn-deserializer))]
  (client/subscribe! consumer "account")
  (client/poll! consumer))
 
```

Async facade:

```clojure
(let [[consumer records ctl] (async/consume! {:bootstrap.servers "localhost:9092"
                                              :group.id          "mygroup"}
                                             (client/keyword-deserializer)
                                             (client/edn-deserializer)
                                             "account")]
  (go
    (loop [record (<! records)]
      (do-something-with record)
      (recur (<! recors)))))
```


