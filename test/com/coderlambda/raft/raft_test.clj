(ns com.coderlambda.raft.raft-test
  (:require [clojure.test :refer :all]
            [com.coderlambda.raft.common :refer :all]
            [com.coderlambda.raft.timers :refer :all]
            [com.coderlambda.raft.cluster :refer :all]
            [com.coderlambda.raft.election :refer :all]
            [com.coderlambda.raft.log-replicate :refer :all]
            [com.coderlambda.raft.raft :refer :all]
            [clojure.core.async :refer [timeout chan go >! <! close! <!!]]))

(deftest test-gen-vote-request
  (let [id "test"
        state (assoc init-state
                     :current-term 3
                     :leader-id "test"
                     :log [(struct log-entry 1 1)
                           (struct log-entry 2 2)
                           (struct log-entry 3 3)])]

    (is (= (gen-vote-request id state)
           {:type :vote-request
            :term 3
            :candidate-id "test"
            :last-log-index 2
            :last-log-term 3})))

  (let [id "test"
        state (assoc init-state
                     :current-term 3
                     :leader-id "test")]
    (is (= (gen-vote-request id state)
           {:type :vote-request
            :term 3
            :candidate-id "test"
            :last-log-index nil
            :last-log-term nil}))))

(deftest test-gen-heartbeat-message
  (let [id "test"
        state (assoc init-state
                     :current-term 3
                     :leader-id "test"
                     :log [(struct log-entry 1 1)
                           (struct log-entry 2 2)
                           (struct log-entry 3 3)]
                     :commit-index 2)]
    (is (= (gen-heartbeat-message id state)
           {:type :append-entries
            :term 3
            :leader-id "test"
            :prev-log-index 2
            :prev-log-term 3
            :entries []
            :leader-commit 2})))

  (let [id "test"
        state (assoc init-state
                     :current-term 3
                     :leader-id "test")]
    (is (= (gen-heartbeat-message id state)
           {:type :append-entries
            :term 3
            :leader-id "test"
            :prev-log-index nil
            :prev-log-term nil
            :entries []
            :leader-commit -1}))))

(deftest test-get-message-handler
  (let [message1 {:type :stop}
        message2 {:type :requestVote}
        state {:running-state :running}
        config {}]
    (is (= ((get-message-handler message1 state config)) {:running-state :stop}))
    (is (= ((get-message-handler message2 state config ())) {:running-state :running}))))


(defn init-cluster [count]
  (let [node-map (for [i (range count)]
                       (struct node-info 
                               (str "node" (+ i 1))
                               (chan)))
        quorum (+ 1 (quot count 2))]
    (struct cluster-info node-map quorum)
))

(defn init-configs [cluster]
  (map (fn [{:keys [id channel]}]
         {:id id
          :channel channel})
       (:node-map cluster)))

(defn close-channels [cluster]
  (doseq [node (:node-map cluster)]
    (close! (:channel node))))

(deftest test-cluster
  (let [cluster (init-cluster 2)
        configs (init-configs cluster)
        stop-message {:type :stop}
        add-log-message {:type :add-log
                         :entries ['(println "hello world")]}
        state (assoc init-state
                     :current-cluster cluster
                     :new-cluster nil)]

    (doseq [config configs]
      (raft config (assoc state
                     :heartbeat-timeout (gen-timeout))))
    (<!!
     (go
       (<! (timeout 1000))
       (println "append entries")
       (doseq [config configs]
         (>! (:channel config) add-log-message))
       (<! (timeout 1000))
       (println "send stop message to all nodes")
       (doseq [config configs]
         (>! (:channel config) stop-message))))
    
    (close-channels cluster)))

