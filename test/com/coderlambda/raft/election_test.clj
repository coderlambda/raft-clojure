(ns com.coderlambda.raft.election-test
  (:require [clojure.test :refer :all]
            [com.coderlambda.raft.common :refer :all]
            [com.coderlambda.raft.election :refer :all]))

(deftest test-handle-election-timeout
  (let [message {:type :election-timeout
                 :term 1}
        config {:id "node1"
                :channel nil}
        three-nodes-cluster  (struct cluster-info
                                 (list (struct node-info "node1" nil)
                                       (struct node-info "node2" nil)
                                       (struct node-info "node3" nil))
                                 2)
        single-node-cluster (struct cluster-info
                                    (list (struct node-info "node1" nil))
                                    1)
        new-cluster nil]
    ;; election timeout
    (let [state (assoc init-state
                       :current-term 1
                       :role :candidate
                       :voted-nodes #{"node1"}
                       :current-cluster three-nodes-cluster
                       :new-cluster nil)
          result (handle-election-timeout message state config)]
      (is (= (:new-state result)
                   (assoc state
                          :current-term 2
                          :role :candidate
                          :voted-nodes #{}
                          :current-cluster three-nodes-cluster
                          :new-cluster nil)))
      (is (= (:response result)
             (gen-vote-request "node1" (:new-state result))))
      (is (= (:target nil))))

    ;; election timeout but election success
    (let [state (assoc init-state
                       :current-term 1
                       :role :candidate
                       :voted-nodes #{"node1"}
                       :current-cluster single-node-cluster
                       :new-cluster nil)
          result (handle-election-timeout message state config)]
      (is (= (:new-state result)
                   (assoc state
                          :current-term 1
                          :role :leader
                          :leader "node1"
                          :voted-nodes #{}
                          :current-cluster single-node-cluster
                          :new-cluster nil)))
      (is (= (:response result)
             (gen-heartbeat-message "node1" (:new-state  result))))
      (is (= (:target nil))))

    ;; current term is higher
    (let [state (assoc init-state
                       :current-term 2
                       :role :candidate
                       :voted-nodes #{"node1"}
                       :current-cluster three-nodes-cluster
                       :new-cluster nil)
          result (handle-election-timeout message state config)]
      (is (= (:new-state result)
             state))
      (is (= (:response result)
             nil))
      (is (= (:target nil))))))

(deftest test-vote-granted?
  (let [config {:id "node1"
                :channel nil}
        three-nodes-cluster  (struct cluster-info
                                 (list (struct node-info "node1" nil)
                                       (struct node-info "node2" nil)
                                       (struct node-info "node3" nil))
                                 2)
        message {:type :vote-request
                 :term 2
                 :candidate-id "node2"
                 :last-log-index 5
                 :last-log-term 3}]
    (let [state init-state]
      (is (vote-granted? message state)))))
