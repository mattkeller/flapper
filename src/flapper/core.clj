(ns flapper.core
  (:require [clojure.core.async :as a]
            [clojure.pprint :as pprint]
            [clojure.spec.alpha :as s])
  (:import [java.util Date]))

;; process-fn takes a channel, gives a channel
;; etc

(s/def ::fn fn?)
(s/def ::name keyword?)
(s/def ::timeout-ms integer?)
(s/def ::async-fn fn?) ; takes 1 arg, returns a channel
(s/def ::stage (s/keys  :opt-un [::name ::fn ::async-fn ::timeout-ms]))
(s/def ::description (s/and (s/coll-of ::stage)
                            vector?))

(defn default-state []
  {:exceptions []
   :timeouts []
   :timing {}
   :start-time (Date.)
   :end-time nil
   :duration-ms nil})


(defn update-state [state-atom stage-key start-ms cnt]
  (let [end-ms (System/currentTimeMillis)
        duration (- end-ms start-ms)]
    (swap! state-atom update-in [:stages stage-key :timing] #(conj % duration))
    (swap! state-atom update-in [:stages stage-key :cnt] (constantly cnt))))

(defn exception-to-state [state-atom stage-key ex v]
  (swap! state-atom update :exceptions conj {:stage stage-key :exception ex :input-value v}))

(defn timeout-to-state [state-atom stage-key v timeout-ms]
  (swap! state-atom update :timeouts conj {:stage stage-key :value v :timeout-ms timeout-ms}))

(defn done-to-state [state-atom]
  (let [start-time (:start-time @state-atom)
        end-time (Date.)
        duration (- (.getTime end-time) (.getTime start-time))]
    (swap! state-atom assoc :end-time end-time)
    (swap! state-atom assoc :duration-ms duration)))

(defn fn-type [_ _ stage]
  (-> stage
      (select-keys [:fn :async-fn])
      keys
      first))

(defn get-stage-key [i {nm :name}]
  (or nm (keyword (str "stage-" i))))

(defmulti wrap-fn fn-type)

;; wrap a non-async function
(defmethod wrap-fn :fn [state-atom i {stage-key :name stage-fn :fn :as stage}]
  (fn [in-ch]
    (let [out-ch (a/chan)
          stage-key (get-stage-key i stage)]
      (a/thread
        (loop [cnt 0]
          (if-let [input (a/<!! in-ch)]
            ;;(println stage-key "in" input)
            (let [start-ms (System/currentTimeMillis)]
              (when-let [output (try
                                  (let [output (stage-fn input)]
                                    (update-state state-atom stage-key start-ms cnt)
                                    output)
                                  (catch Throwable t
                                    (exception-to-state state-atom stage-key t input)
                                    nil))]
                ;;(println stage-key "out" output)
                (a/>!! out-ch output))
              (recur (inc cnt)))
            (a/close! out-ch))))

      out-ch)))

;; wrap a function that takes 1 arg and reurns a channel - a "async" fn
(defmethod wrap-fn :async-fn [state-atom i {stage-key :name stage-fn :async-fn timeout-ms :timeout-ms :as stage}]
  (fn [in-ch]
    (let [out-ch (a/chan)
          stage-key (get-stage-key i stage)]
      (a/go-loop [cnt 0]
        (if-let [input (a/<! in-ch)]
          ;;(println stage-key "in" input)
          (let [start-ms (System/currentTimeMillis)
                stage-fn-ch (try
                              (stage-fn input)
                              (catch Throwable t
                                (exception-to-state state-atom stage-key t input)
                                nil))]
            (when stage-fn-ch
              (let [to-ch (when timeout-ms (a/timeout timeout-ms))
                    channels (remove nil? [stage-fn-ch to-ch])
                    [output ready-ch] (a/alts! channels)]
                (if (= stage-fn-ch ready-ch)
                  (when output
                    ;;(println stage-key "out" output)
                    (update-state state-atom stage-key start-ms cnt)
                    (a/>! out-ch output))
                  (timeout-to-state state-atom stage-key input timeout-ms))))
            (recur (inc cnt)))
          (a/close! out-ch)))

      out-ch)))

;; TODO some users will want to drain, others to consume a channel of results
(defn drain [state-atom in-ch]
  (a/go-loop []
    (if-let [v (a/<! in-ch)]
      (do
        (println "Done" v)
        (recur))
      (done-to-state state-atom))))

(defn compose-stages [desc]
  (when-not (s/valid? ::description desc)
    (throw (ex-info "Invalid pipeline description" (s/explain-data ::description desc))))
  (let [state (atom (default-state))
        wrapped-stages (->> (rest desc)
                            (map-indexed
                             (fn [i stage]
                               (let [index (inc i)]
                                 (swap! state assoc-in [:stages (get-stage-key index stage) :index] index)
                                 (wrap-fn state index stage))))
                            vec)]
    {:pipeline
     (fn []
       ;; invoke 1st stage, the source. Force returned value to a channel.
       (let [first-stage (first desc)
             src (try
                   ((:fn first-stage))
                   (catch Throwable t
                     (exception-to-state state (get-stage-key 0 first-stage) t nil)
                     nil))
             src-ch (cond
                      (instance? clojure.core.async.impl.protocols.Channel src)
                      src

                      (seq? src)
                      (a/to-chan src)

                      :else
                      (a/go src))]

         ;; wire intermediate stages together
         (drain
          state
          (loop [in-ch src-ch
                 processes wrapped-stages]
            (if-let [p (first processes)]
              (recur (p in-ch)
                     (rest processes))
              in-ch)))))

     :state state}))


;; testing

(defn async1 [x]
  (a/go x))

(defn async2 [x]
  (a/go
    (a/<! (a/timeout 90))
    x))


(def test-stages
  [{:name :src  :fn #(range 1 11)}
   {:name :filt :fn #(when-not (= % 5) %)}
   ;;{:name :src :fn #(throw (ex-info "boom" {}))}
   {:name :mult :fn #(* % 2)}
   ;;{:name :boom :fn (fn [_] (throw (NullPointerException. "npe")))}
   {:name :a1   :async-fn async1}
   {:name :a2   :async-fn async2 :timeout-ms 100}
   {:name :add1 :fn #(inc %)}])

(defn t []
  (let [{:keys [pipeline state]}
        (compose-stages test-stages)]
    (pprint/pprint test-stages)
    (pipeline)
    state))


