(ns onyx-reducers.deduplicate
  (:require [taoensso.nippy :as nippy])
  (:import [com.clearspring.analytics.stream.membership BloomFilter]))

(nippy/extend-freeze BloomFilter :onyx-reducers/bloom-filter
  [x data-output]
  (.write data-output (BloomFilter/serialize x)))

(nippy/extend-thaw :onyx-reducers/bloom-filter
  [data-input]
  (BloomFilter/deserialize (.readFully data-input)))

(defn deduplicate-fn-init [{:keys [bloom/num-elements bloom/buckets-per-element]}]
  {:filter (BloomFilter. num-elements buckets-per-element)})

(defn deduplicate-create-state-update-fn [window state segment]
  segment)

(defn deduplicate-apply-state-update-fn [window state v]
  (if-not (.isPresent (:filter state) (pr-str (:id v)))
    (do
     (.add (:filter state) (pr-str (:id v)))
     (assoc state :emit v))
    (do
     ;; re-up for recency
     (.add (:filter state) (pr-str (:id v)))
     (assoc state :emit nil))))

(def ^:export deduplicate
  {:aggregation/init deduplicate-fn-init
   :aggregation/create-state-update deduplicate-create-state-update-fn
   :aggregation/apply-state-update deduplicate-apply-state-update-fn})
