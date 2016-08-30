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
  (let [stopper (when stop (fn [] (a/close! stop)))
        opts    (dissoc config :input-buffer :output-buffer :timeout)]
    (cond
      (and stop kd vd) (client/consumer opts stopper kd vd)
      stop             (client/consumer opts stopper)
      (and kd vd)      (client/consumer opts kd vd)
      :else            (client/consumer opts))))

(defn consume!
  "[DEPRECATED] You should now prefer the `consumer` function.

   Build a consumer for a topic or list of topics which
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
         consumer (make-consumer config nil kd vd)
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
  "Build an async consumer. Yields a vector of record and control
   channels.

   Arguments config ks and vs work as for kinsky.client/consumer.
   The config map must be a valid consumer configuration map and may contain
   the following additional keys:

   - `:input-buffer`: Maximum backlog of control channel messages.
   - `:output-buffer`: Maximum queued consumed messages.
   - `:timeout`: Poll interval


   The resulting control channel is used to interact with the consumer driver
   and expects map payloads, whose operation is determined by their
   `:op` key. The following commands are handled:

   - `:subscribe`: `{:op :subscribe :topic \"foo\"}` subscribe to a topic.
   - `:unsubscribe`: `{:op :unsubscribe}`, unsubscribe from all topics.
   - `:partitions-for`: `{:op :partitions-for :topic \"foo\"}`, yield partition
      info for the given topic. If a `:response` key is
      present, produce the response there instead of on
      the record channel.
   - `commit`: `{:op :commit}` commit offsets, an optional `:topic-offsets` key
      may be present for specific offset committing.
   - `:pause`: `{:op :pause}` pause consumption.
   - `:resume`: `{:op :resume}` resume consumption.
   - `:calllback`: `{:op :callback :callback (fn [d ch])}` Execute a function
      of 2 arguments, the consumer driver and output channel, on a woken up
      driver.
   - `:stop`: `{:op :stop}` stop and close consumer.


   The resulting output channel will emit payloads with as maps containing a
   `:type` key where `:type` may be:

   - `:record`: A consumed record.
   - `:exception`: An exception raised
   - `:rebalance`: A rebalance event.
   - `:eof`: The end of this stream.
   - `:partitions`: The result of a `:partitions-for` operation.
   - `:woken-up`: A notification that the consumer was woken up.
"
  ([config]
   (consumer config nil nil))
  ([config kd vd]
   (let [inbuf    (or (:input-buffer config) default-input-buffer)
         outbuf   (or (:output-buffer config) default-output-buffer)
         timeout  (or (:timeout config) default-timeout)
         ctl      (a/chan inbuf)
         recs     (a/chan outbuf record-xform (fn [e] (throw e)))
         out      (a/chan outbuf)
         listener (channel-listener out)
         driver   (make-consumer config nil kd vd)
         next!    (next-poller driver timeout)]
     (a/pipe recs out false)
     (a/go
       (loop [[poller payload] [(next!) nil]]
         (let [[v c] (a/alts! (if payload
                                [ctl [recs payload]]
                                [ctl poller]))]
           (condp = c
             ctl    (let [{:keys [op topic topics topic-offsets
                                  response topic-partitions callback]
                           :as payload} v]
                      (try
                        (client/wake-up! driver)
                        (when-let [records (a/<! poller)]
                          (a/>! recs records))

                        (cond
                          (= op :callback)
                          (callback driver out)

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
                          (a/>! (or response out)
                                {:type       :partitions
                                 :partitions (client/partitions-for driver topic)})

                          (= op :stop)
                          (do (a/>! out {:type :eof})
                              (client/close! driver)
                              (a/close! out)))
                        (catch Exception e
                          (do (a/>! out {:type :exception :exception e})
                              (client/close! driver)
                              (a/close! out))))
                      (when (not= op :stop)
                        (recur [poller payload])))
             poller (recur [(next!) v])
             recs   (recur [poller nil])))))
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
   The config map must be a valid consumer configuration map and may contain
   the following additional keys:

   - `:input-buffer`: Maximum backlog of control channel messages.
   - `:output-buffer`: Maximum queued consumed messages.

   Yields a vector of two values `[in out]`, an input channel and
   an output channel.

   The resulting input channel is used to interact with the producer driver
   and expects map payloads, whose operation is determined by their
   `:op` key. The following commands are handled:

   - `:record`: `{:op :record :topic \"foo\"}` send a record out, also
      performed when no `:op` key is present.
   - `:flush`: `{:op :flush}`, flush unsent messages.
   - `:partitions-for`: `{:op :partitions-for :topic \"foo\"}`, yield partition
      info for the given topic. If a `:response` key is
      present, produce the response there instead of on
      the record channel.
   - `:close`: `{:op :close}`, close the producer.

   The resulting output channel will emit payloads with as maps containing a
   `:type` key where `:type` may be:

   - `:exception`: An exception raised
   - `:partitions`: The result of a `:partitions-for` operation.
   - `:eof`: The producer is closed.
"
  ([config]
   (producer config nil nil))
  ([config ks]
   (producer config ks ks))
  ([config ks vs]
   (let [inbuf   (or (:input-buffer config) default-input-buffer)
         outbuf  (or (:output-buffer config) default-output-buffer)
         opts    (dissoc config :input-buffer :output-buffer)
         driver  (make-producer opts ks vs)
         in      (a/chan inbuf)
         out     (a/chan outbuf)]
     (a/go-loop []
       (let [{:keys [op timeout callback response topic]
              :as   record}
             (a/<! in)]
         (try
           (cond
             (= op :close)
             (do (if timeout
                   (client/close! driver timeout)
                   (client/close! driver))
                 (a/>! out {:type :eof})
                 (a/close! out))

             (= op :flush)
             (client/flush! driver)

             (= op :callback)
             (callback driver)

             (= op :partitions-for)
             (a/>! (or response out)
                   {:type       :partitions
                    :partitions (client/partitions-for driver (name topic))})

             :else
             (client/send! driver (dissoc record :op)))
           (catch Exception e
             (a/>! out {:type :exception :exception e})))
         (when (not= type :close)
           (recur))))
     [in out])))
