(ns onyx-reducers.deduplicate-aggregate-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [schema.core :as s]
            [onyx.schema :as os]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.api]))

(def input
  [{:id 1}
   {:id 2}
   {:id 3}
   {:id 4}
   {:id 5}
   {:id 6}
   {:id 7}
   {:id 8}
   {:id 8}
   {:id 8}
   {:id 9}
   {:id 10}
   {:id 11}
   {:id 12}
   {:id 10}
   {:id 10}
   {:id 10}
   {:id 10}
   {:id 10}
   {:id 13}
   {:id 14}
   {:id 15}])

(s/defn emit-segment [event :- os/Event 
                      window :- os/Window 
                      trigger :- os/Trigger 
                      {:keys [lower-bound upper-bound event-type] :as state-event} :- os/StateEvent 
                      extent-state]
  (when-let [emit (:emit extent-state)] 
    {:event-type event-type
     :window/id (:window/id window)
     :trigger/id (:trigger/id trigger)
     :state emit}))

(def in-chan (atom nil))
(def in-buffer (atom nil))

(def out-chan (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/buffer in-buffer
   :core.async/chan @in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(deftest conj-test
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        batch-size 20
        workflow
        [[:in :identity] [:identity :out]]

        catalog
        [{:onyx/name :in
          :onyx/plugin :onyx.plugin.core-async/input
          :onyx/type :input
          :onyx/medium :core.async
          :onyx/batch-size batch-size
          :onyx/max-peers 1
          :onyx/doc "Reads segments from a core.async channel"}

         {:onyx/name :identity
          :onyx/fn :clojure.core/identity
          :onyx/type :function
          :onyx/max-peers 1
          :onyx/batch-size batch-size}

         {:onyx/name :out
          :onyx/plugin :onyx.plugin.core-async/output
          :onyx/type :output
          :onyx/medium :core.async
          :onyx/batch-size batch-size
          :onyx/max-peers 1
          :onyx/doc "Writes segments to a core.async channel"}]

        windows
        [{:window/id :collect-segments
          :window/task :identity
          :window/type :global
          :bloom/num-elements 1000000
          :bloom/buckets-per-element 10
          :window/aggregation :onyx-reducers.deduplicate/deduplicate}]

        triggers
        [{:trigger/window-id :collect-segments
          :trigger/id :emit-aggregates
          :trigger/refinement :onyx.refinements/accumulating
          :trigger/fire-all-extents? true
          :trigger/on :onyx.triggers/segment
          :trigger/threshold [1 :elements]
          :trigger/emit ::emit-segment}]

        lifecycles
        [{:lifecycle/task :in
          :lifecycle/calls ::in-calls}
         {:lifecycle/task :out
          :lifecycle/calls ::out-calls}]]

    (reset! in-chan (chan (inc (count input))))
    (reset! in-buffer {})
    (reset! out-chan (chan 5000))

    (with-test-env [test-env [3 env-config peer-config]]
      (doseq [i input]
        (>!! @in-chan i))
        (close! @in-chan)
        (let [job (onyx.api/submit-job
                   peer-config
                   {:catalog catalog
                    :workflow workflow
                    :lifecycles lifecycles
                    :windows windows
                    :triggers triggers
                    :task-scheduler :onyx.task-scheduler/balanced})
              _ (onyx.test-helper/feedback-exception! peer-config (:job-id job))
              results (take-segments! @out-chan 50)]
          (is (= (distinct (sort-by :id input)) (sort-by :id (map :state (filter :event-type results)))))

          
          ))))
