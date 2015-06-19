(ns com.coderlambda.raft.raft-test
  (:require [clojure.test :refer :all]
            [com.coderlambda.raft.raft :refer :all]
            [clojure.core.async :as async :refer [timeout chan go >! <! close! <!!]]))

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


(deftest test-handle-election-timeout
  (let [message {:type :election-timeout :term 2}
        state (assoc init-state :current-term 3 :role :follower)
        config {:id "test" :quorum 2}]
    (is (= (handle-election-timeout message state config)
           (struct handler-result
                   state nil nil))))

  (let [message {:type :election-timeout :term 2}
        state (assoc init-state :role :leader)
        config {:id "test" :quorum 2}]
    (is (= (handle-election-timeout message state config)
           (struct handler-result
                   state nil nil))))
  
  (let [message {:type :election-timeout :term 2}
        config {:id "test" :quorum 1
                :cluster-info [{:id "test"}]}
        state (assoc init-state
                     :role :candidate
                     :current-term 2
                     :vote-for "test"
                     :granted-vote-count 1)
        new-state (assoc init-state
                         :role :leader
                         :leader "test"
                         :current-term 2
                         :granted-vote-count 0
                         :vote-for "test"
                         :next-index {"test" 0}
                         :match-index {"test" 0})]
    (is (= (handle-election-timeout message state config)
           (struct handler-result
                   new-state
                   (gen-heartbeat-message (:id config) new-state)
                   nil))))

  (let [message {:type :election-timeout :term 2}
        config {:id "test" :quorum 2
                :cluster-info [{:id "test"}]}
        state (assoc init-state
                     :role :candidate
                     :current-term 2
                     :vote-for "test"
                     :granted-vote-count 1)
        new-state (assoc init-state
                         :role :candidate
                         :current-term 3
                         :granted-vote-count 1
                         :vote-for "test")]
    (is (= (handle-election-timeout message state config)
           (struct handler-result
                   new-state
                   (gen-vote-request (:id config) new-state)
                   nil)))))

(deftest test-handle-vote-request
  ;; request's term is equal to current term and not vote for anyone
  (let [message {:term 1
                 :candidate-id "node2"}
        state (assoc init-state 
                     :current-term 1
                     :vote-for nil)
        config {:id "node1"}]
    (is (= (handle-vote-request message state config)
           (assoc init-state
                  :current-term 1
                  :vote-for "node2"))))
  
  ;; request's term is equal to current term but this node has vote for someone else
  (let [message {:term 1
                 :candidate-id "node2"}
        state (assoc init-state 
                     :current-term 1
                     :vote-for "node3")
        config {:id "node1"}]
    (is (= (handle-vote-request message state config)
           (assoc init-state
                  :current-term 1
                  :vote-for "node3"))))
  
  ;; request's term is smaller than current term
  (let [message {:type :vote-request
                  :term 1
                  :candidate-id "node2"}
        state (assoc init-state
                     :current-term 2
                     :vote-for nil)
        config {:id "node1"}]
    (is (= (handle-vote-request message state config)
           (assoc init-state
                  :current-term 2
                  :vote-for nil))))

  ;; request's term is bigger than current term
  (let [message {:type :vote-request
                 :term 3
                 :candidate-id "node2"}
        state (assoc init-state
                     :current-term 2
                     :vote-for nil)
        config {:id "node1"}]
    (is (= (handle-vote-request message state config)
           (assoc init-state
                  :current-term 3
                  :vote-for "node2")))))



(deftest test-handle-vote-request-response
   (let [message {:type :vote-request-response
                  :term 1
                  :vote-granted true}
        state (assoc init-state
                     :role :candidate
                     :current-term 1
                     :vote-for "node1")
        config {:id "node1"
                :quorum 2}]
    (is (= (handle-vote-request-response message state config)
           (assoc state
                  :granted-vote-count (+ (:granted-vote-count state) 1)))))

   (let [message {:type :vote-request-response
                  :term 1
                  :vote-granted true}
        state (assoc init-state
                     :role :candidate
                     :current-term 1
                     :granted-vote-count 1
                     :vote-for "node1")
        config {:id "node1"
                :quorum 2}]
    (is (= (handle-vote-request-response message state config)
           (assoc state
                  :granted-vote-count 0
                  :vote-for nil
                  :role :leader)))))






(deftest test-get-message-handler
  (let [message1 {:type :stop}
        message2 {:type :requestVote}
        state {:running-state :running}
        config {}]
    (is (= ((get-message-handler message1 state config)) {:running-state :stop}))
    (is (= ((get-message-handler message2 state config ())) {:running-state :running}))))


(defn init-configs [count]
  (let [cluster-info (for [i (range count)]
                       (struct cluster-info 
                               (str "node" (+ i 1))
                               (chan)))
        quorum (+ 1 (quot count 2))]
    (map (fn [{:keys [id channel]}]
           (struct config id channel cluster-info quorum))
         cluster-info)))

(defn close-channels [config]
  (let [cluster-info (:cluster-info config)]
    (doseq [info cluster-info]
      (close! (:channel info)))))

(deftest test-cluster
  (let [configs (init-configs 3)
        stop-message {:type :stop}
        add-log-message {:type :add-log
                         :entries ['(println "hello world")]}
        state init-state]
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
    
    (close-channels [get configs 0])))

(deftest test-raft
  (let [config {:id "node1"
                :channel (chan)
                :quorum 1
                :heartbeat-timeout 200}
        message2 {:type :stop}
        state init-state]
    
    (raft config state)
    (go
      (println "wait for 5s")
      (<! (timeout 5000))
      (println "send stop message to raft")
      (>! (:channel config) message2))))
