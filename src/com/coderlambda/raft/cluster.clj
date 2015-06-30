(ns com.coderlambda.raft.cluster
  (:require [clojure.core.async :as async :refer [go >!]]
            [clojure.set :refer [intersection]]
            [com.coderlambda.raft.common :as common :refer [node-info cluster-info]]))

(defn combine-cluster-nodes [current-cluster new-cluster]
  (if (nil? new-cluster)
    (:node-map current-cluster)
    (distinct (concat (:node-map new-cluster) (:node-map current-cluster)))))

(defn broadcast-message [message current-cluster new-cluster]
  (let [nodes (combine-cluster-nodes current-cluster new-cluster)]
    (doseq [target nodes]
      (go (>! (:channel target) message)))))

(defn send-message [message target current-cluster new-cluster]
  (let [nodes (combine-cluster-nodes current-cluster new-cluster)]
    (doseq [node nodes]
      (if (= (:id node) target)
        (go (>! (:channel node) message))))))

(defn get-cluster-node-id-set [cluster]
  (into #{} (map #(:id %1) (:node-map cluster))))

(defn majority? [nodes cluster]
  (>= (count (intersection nodes (get-cluster-node-id-set cluster)))
      (:quorum cluster)))

(defn majority-in-both? [nodes & clusters]
  (reduce (fn [result cluster]
            (and result
                 (if (nil? cluster)
                   true
                   (majority? nodes cluster))))
          true clusters))
