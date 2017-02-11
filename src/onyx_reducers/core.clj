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
  ;; we're going to need a serialize / restore method, along with the replicated log machine method
  (HyperLogLog$Builder/build (.getBytes hll))
  )
