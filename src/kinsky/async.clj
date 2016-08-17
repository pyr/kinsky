(ns kinsky.async
  "Clojure core.async support in kinsky.
   See https://github.com/pyr/kinsky for example usage."
  (:require [clojure.core.async :as a]
            [kinsky.client      :as client]))

(def default-input-buffer
  "Default amount of messages buffered on control channels."
  10)

(def default-output-buffer
  "Default amount of messages buffered on the record channel."
  100)

(def default-timeout
  "Default timeout, by default we poll at 100ms intervals."
  100)

(defn channel-listener
  "A rebalance-listener compatible call back which produces all
   events onto a channel."
  [ch]
  (fn [event]
    (a/put! ch (assoc event :type :rebalance))))

(defn next-poller
  "Poll for next messages, catching exceptions and yielding them."
  [consumer timeout]
  (fn []
    (a/thread
      (try
        (let [out (client/poll! consumer timeout)]
          out)
        (catch org.apache.kafka.common.errors.WakeupException we
          {:type :woken-up})
        (catch Exception e
          {:type :exception :exception e})))))

(defn exception?
  "Test if a value is a subclass of Exception"
  [e]
  (instance? Exception e))

(defn make-consumer
  "Build a consumer, with or without deserializers"
  [config stop kd vd]
  (let [stopper (fn [] (a/close! stop))
        opts    (dissoc config :input-buffer :output-buffer :timeout)]
    (cond
      (and kd vd) (client/consumer opts stopper kd vd)
      :else       (client/consumer opts stopper))))

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


(def record-xform
  "Rely on the standard transducer but indicate that this is a record."
  (comp client/record-xform
        (map #(assoc % :type :record))))

(defn consumer
  "Build an async consumer. Similar to `consume!`. "
  ([config]
   (consumer config nil nil))
  ([config kd vd]
   (let [inbuf    (or (:input-buffer config) default-input-buffer)
         outbuf   (or (:output-buffer config) default-output-buffer)
         timeout  (or (:timeout config) default-timeout)
         ctl      (a/chan inbuf)
         recs     (a/chan outbuf record-xform (fn [e] (throw e)))
         out      (a/chan outbuf)
         stop     (a/promise-chan)
         driver   (make-consumer config ctl kd vd)
         listener (channel-listener out)
         next!    (next-poller driver timeout)]
     (a/pipe recs out false)
     (a/go
       (loop [poller (next!)]
         (a/alt!
           stop   ([_]
                   (a/>! out {:type :eof})
                   (a/close! out))
           ctl    ([{:keys [op topic topics topic-offsets
                            response topic-partitions]
                     :as payload}]
                   (try
                     (client/wake-up! driver)
                     (when-let [records (a/<! poller)]
                       (a/>! recs records))

                     (cond
                       (= op :subscribe)
                       (client/subscribe! driver (or topics topic) listener)

                       (= op :unsubscribe)
                       (client/unsubscribe! driver)

                       (and (= op :commit) topic-offsets)
                       (client/commit! driver topic-offsets)

                       (= op :commit)
                       (client/commit! driver)

                       (= op :pause)
                       (client/pause! driver topic-partitions)

                       (= op :resume)
                       (client/resume! driver topic-partitions)

                       (= op :partitions-for)
                       (a/put! (or response out)
                               {:type       :partitions
                                :partitions (client/partitions-for driver topic)})


                       (= op :stop)
                       (do (a/put! out {:type :eof}) (a/close! out)))
                     (catch Exception e
                       (println "control channel exception:" e)))
                   (when (not= op :stop)
                     (recur poller)))
           poller ([payload]
                   (when payload
                     (a/put! recs payload))
                   (recur (next!))))))
     [out ctl])))

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

(comment

  (def record-ch
    (let [[out ctl] (consumer {:bootstrap.servers "localhost:9092"}
                              (client/string-deserializer)
                              (client/string-deserializer))]

      (a/go-loop []
        (when-let [record (a/<! out)]
          (println (pr-str record))
          (recur)))
      ctl))


  (a/put! record-ch {:op :partitions-for :topic "kafkanary"})
  (a/put! record-ch {:op :subscribe :topic "testing"})

  (a/put! record-ch {:op :stop})
)
