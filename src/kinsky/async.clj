(ns kinsky.async
  "core.async support in kinsky"
  (:require [clojure.core.async :as a]
            [kinsky.client      :as client]))

(def record-xform
  "A transducer to explode grouped records into individual
   entities.

   When sucessful, the output of kinsky.client/poll! takes the
   form:

       {:partitions   [[\"t\" 0] [\"t\" 1]]
        :topics       #{\"t\"}
        :count        2
        :by-partition {[\"t\" 0] [{:key       \"k0\"
                                   :offset    1
                                   :partition 0
                                   :topic     \"t\"
                                   :value     \"v0\"}]
                       [\"t\" 1] [{:key       \"k1\"
                                   :offset    1
                                   :partition 1
                                   :topic     \"t\"
                                   :value     \"v1\"}]}
        :by-topic      {\"t\" [{:key       \"k0\"
                                :offset    1
                                :partition 0
                                :topic     \"t\"
                                :value     \"v0\"}
                               {:key       \"k1\"
                                :offset    1
                                :partition 1
                                :topic     \"t\"
                                :value     \"v1\"}]}}

   To make working with the output channel easier, this
   transducer morphs these messages into a list of
   distinct records:

       ({:key \"k0\" :offset 1 :partition 0 :topic \"t\" :value \"v0\"}
        {:key \"k1\" :offset 1 :partition 1 :topic \"t\" :value \"v1\"}
        ...)
"
  (mapcat (comp (partial mapcat identity)
                vals
                :by-partition)))

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

   - consumer is a consumer driver, see kinsky.client/consumer
   - records is a channel of records, see kinsky.async/record-xform
     for content description
   - ctl is a channel of control messages, as given by the rebalance
     listener or exception if they were produced by the transducer.
   "
  ([config topics]
   (consume! config nil nil topics))
  ([config kd vd topics]
   (let [out      (a/chan 10 record-xform (fn [e] (throw e)))
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
