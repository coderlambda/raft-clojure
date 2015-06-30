(ns com.coderlambda.raft.cluster-test
  (:require [clojure.test :refer :all]
            [com.coderlambda.raft.cluster :refer :all]
            [com.coderlambda.raft.common :refer :all]
            [clojure.core.async :as async :refer [timeout chan go >! <! close! <!!]]))

(deftest test-combine-cluster-nodes
  (let [current-cluster (struct cluster-info
                                (list (struct node-info "node1" nil)
                                      (struct node-info "node2" nil)
                                      (struct node-info "node3" nil))
                                2)
        new-cluster nil]
    (is (= (map #(:id %1) (combine-cluster-nodes current-cluster new-cluster))
           '("node1" "node2" "node3"))))

   (let [current-cluster (struct cluster-info
                                 (list (struct node-info "node1" nil)
                                       (struct node-info "node2" nil)
                                       (struct node-info "node3" nil))
                                 2)
        new-cluster  (struct cluster-info
                                 (list (struct node-info "node2" nil)
                                       (struct node-info "node3" nil)
                                       (struct node-info "node4" nil))
                                 2)
         node-ids (map #(:id %1) (combine-cluster-nodes current-cluster new-cluster))
         node-ids-set (into #{} node-ids)]
     (is (and (= (count node-ids)
                 (count node-ids-set))
              (= node-ids-set
                 #{"node1" "node2" "node3" "node4"}))))
      (let [current-cluster (struct cluster-info
                                 (list (struct node-info "node1" nil)
                                       (struct node-info "node2" nil)
)
                                 2)
        new-cluster  (struct cluster-info
                                 (list (struct node-info "node4" nil))
                                 2)
         node-ids (map #(:id %1) (combine-cluster-nodes current-cluster new-cluster))
         node-ids-set (into #{} node-ids)]
     (is (and (= (count node-ids)
                 (count node-ids-set))
              (= node-ids-set
                 #{"node1" "node2" "node4"}))))
   )


(deftest test-majority?
  (let [cluster (struct cluster-info
                        (list (struct node-info "node1" nil)
                              (struct node-info "node2" nil)
                              (struct node-info "node3" nil))
                        2)]
    (is (not (majority? #{} cluster)))
    (is (not (majority? #{"node1"} cluster)))
    (is (majority? #{"node1" "node2"} cluster))
    (is (majority? #{"node1" "node2" "node3"} cluster))
    (is (majority? #{"node2" "node3"} cluster))))

(deftest test-majority-in-both?
  (let [cluster1 nil
        cluster2 (struct cluster-info
                         (list (struct node-info "node2" nil)
                               (struct node-info "node3" nil)
                               (struct node-info "node4" nil))
                         2)]
    (is (not (majority-in-both? #{} cluster1 cluster2)))
    (is (not (majority-in-both? #{"node2"} cluster1 cluster2)))
    (is (majority-in-both? #{"node2" "node3"} cluster1 cluster2)))

  (let [cluster1 (struct cluster-info
                         (list (struct node-info "node2" nil)
                               (struct node-info "node3" nil)
                               (struct node-info "node4" nil))
                         2)
        cluster2 nil]
    (is (majority-in-both? #{"node2" "node3"} cluster1 cluster2)))

  (let [cluster1 (struct cluster-info
                         (list (struct node-info "node1" nil)
                               (struct node-info "node2" nil)
                               (struct node-info "node3" nil))
                         2)
        cluster2 (struct cluster-info
                         (list (struct node-info "node2" nil)
                               (struct node-info "node3" nil)
                               (struct node-info "node4" nil))
                         2)]
    (is (not (majority-in-both? #{} cluster1 cluster2)))
    (is (not (majority-in-both? #{"node1"} cluster1 cluster2)))
    (is (not (majority-in-both? #{"node2"} cluster1 cluster2)))
    (is (not (majority-in-both? #{"node4"} cluster1 cluster2)))
    (is (not (majority-in-both? #{"node1" "node2"} cluster1 cluster2)))
    (is (not (majority-in-both? #{"node1" "node3"} cluster1 cluster2)))
    (is (not (majority-in-both? #{"node1" "node4"} cluster1 cluster2)))
    (is (majority-in-both? #{"node2" "node3"} cluster1 cluster2))
    (is (majority-in-both? #{"node1" "node2" "node3"} cluster1 cluster2))
    (is (majority-in-both? #{"node1" "node3" "node4"} cluster1 cluster2))))


