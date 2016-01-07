(ns kinsky.async
  "Clojure core.async support in kinsky.
   See https://github.com/pyr/kinsky for example usage."
  (:require [clojure.core.async :as a]
            [kinsky.client      :as client]))

(defn channel-listener
  "A rebalance-listener compatible call back which produces all
   events onto a channel."
  [ch]
  (fn [event]
    (a/put! ch event)))

(defn next-poller
  "Poll for next messages, catching exceptions and yielding them."
  [consumer timeout]
  (fn []
    (a/thread
      (try
        (client/safe-poll! consumer 100)
        (catch Exception e
          e)))))

(defn exception?
  "Test if a value is a subclass of Exception"
  [e]
  (instance? Exception e))

(defn make-consumer
  "Build a consumer, with or without deserializers"
  [config stop kd vd]
  (let [
        stopper    (fn [] (a/close! stop))]
    (cond
      (and kd vd) (client/consumer config stopper kd vd)
      :else       (client/consumer config stopper))))

(defn consume!
  "Build a consumer for a topic or list of topics which
   will produce records onto a channel.

   Arguments config, kd and vd work as for kinsky.client/consumer
   Argument topics is as for kinsky.client/subscribe

   Yields a vector of three values: [consumer records ctl]

   - consumer is a consumer driver, see
     [kinsky.client/consumer](kinsky.client.html#var-consumer)
   - records is a channel of records, see
     [kinsky.client/record-xform](kinsky.client.html##var-record-xform)
     for content description
   - ctl is a channel of control messages, as given by the rebalance
     listener or exception if they were produced by the transducer.
   "
  ([config topics]
   (consume! config nil nil topics))
  ([config kd vd topics]
   (let [out      (a/chan 10 client/record-xform (fn [e] (throw e)))
         ctl      (a/chan 10)
         stop     (a/promise-chan)
         consumer (make-consumer config stop kd vd)
         next!    (next-poller consumer 100)]
     (client/subscribe! consumer topics (channel-listener ctl))
     (a/go
       (loop [records (next!)]
         (a/alt!
           stop    ([_]
                    (a/>! ctl {:type :eof})
                    (a/close! ctl)
                    (a/close! out))
           records ([v]
                    (cond
                      (exception? v) (a/>! ctl {:type :exception :exception v})
                      v              (a/>! out v))
                    (recur (next!))))))
     [consumer out ctl])))

(defn make-producer
  "Build a producer, with or without serializers"
  [config ks vs]
  (cond
    (and ks vs) (client/producer config ks vs)
    :else       (client/producer config)))

(defn producer
  "Build a producer, reading records to send from a channel.

   Arguments config ks and vs work as for kinsky.client/producer.

   Yields a vector of three values: [producer records ctl]

   - producer is a producer driver, see:
     [kinsky.client/producer](kinsky.client.html#var-producer)
   - records expects records to produce, see:
     [kinsky.client/send!](kinsky.client.html#var-send.21)
   - ctl is a channel which will receive any exception received
     during production."
  ([config]
   (producer config nil nil))
  ([config ks]
   (producer config ks ks))
  ([config ks vs]
   (let [producer (make-producer config ks vs)
         records  (a/chan 10)
         ctl      (a/chan 10)]
     (a/go
       (loop [record (a/<! records)]
         (when record
           (try
             (client/send! producer record)
             (catch Exception e
               (a/>! ctl e)))
           (recur (a/<! records)))))
     [producer records ctl])))
