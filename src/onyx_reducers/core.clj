(ns onyx-reducers.core
  (:import [com.clearspring.analytics.stream.cardinality HyperLogLog HyperLogLog$Builder]))

(let [relative-std-deviation 16
      hll (HyperLogLog. relative-std-deviation)] 
  (.offer hll "sien")
  (.offer hll 5)
  (.offer hll 59)
  (.offer hll 59)
  (.offer hll 59)
  (.offer hll 59)
  (.offer hll 59)
  (.offer hll 59)
  (.offer hll 59)
  (.offer hll 59)
  (.offer hll 59)
  (.offer hll 59)
  (.offer hll 59)
  (.offer hll 59)
  (.cardinality hll)
  (HyperLogLog$Builder/build (.getBytes hll))
  )
