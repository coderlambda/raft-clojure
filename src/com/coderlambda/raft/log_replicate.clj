(ns com.coderlambda.raft.log-replicate
  (:require [com.coderlambda.raft.common :refer [handler-result print-log]]
            [com.coderlambda.raft.cluster :refer [get-cluster-node-id-set]]
            [com.coderlambda.raft.election :refer [become-follower
                                                   handle-message-term-bigger-than-current
                                                   gen-heartbeat-message]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; rpc handlers
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn gen-append-entries-response-message [id state success]
  (let [{:keys [current-term log]} state]
    {:type :append-entries-response
     :follower-id id
     :term current-term
     :success success
     :last-log-index (dec (count log))}))

(defn reset-log-entries [state entries]
  (assoc state
         :log entries
         :commit-index -1
         :last-applied -1))

(defn prev-log-match? [logs {:keys [prev-log-index prev-log-term]}]
  (and (< prev-log-index (count logs))
       (= prev-log-term (:term (nth logs prev-log-index)))))

(defn append-log-entries [state {:keys [prev-log-index leader-commit entries]}]
  (let [{:keys [log]} state
        log (reduce conj (subvec log 0 (inc prev-log-index)) entries)
        new-commit-index (min leader-commit (dec (count log)))]
    (assoc state
           :log log
           :commit-index new-commit-index)))

(defn handle-append-entries [message old-state config]
  (let [{:keys [id]} config
        {:keys [term leader-id prev-log-index prev-log-term entries leader-commit]} message
        {:keys [role current-term leader log commit-index]} old-state
        state (cond (> term current-term)
                    (assoc (become-follower old-state config)
                           :current-term term
                           :leader leader-id)

                    (= term current-term)
                    (case role
                      :leader old-state
                      :candidate (assoc (become-follower old-state config)
                                        :leader leader-id)
                      :follower (assoc old-state
                                       :leader leader-id
                                       :heartbeat-timestamp (System/currentTimeMillis)))
                    
                    :else old-state)
        new-state (cond 
                    (not prev-log-index) (reset-log-entries state entries)
                    (prev-log-match? log message) (append-log-entries state message)
                    :else nil)]
    (if (nil? new-state)
      (struct handler-result
              state
              (gen-append-entries-response-message id state false)
              leader-id)
      (struct handler-result
              new-state
              (gen-append-entries-response-message id new-state true)))))

(defn gen-append-entries-message [id state next-index]
  (let [{:keys [current-term log commit-index]} state
        prev-log-index (if (= next-index 0) nil (dec next-index)) 
        prev-log-term (if prev-log-index (:term (nth log prev-log-index)) nil)
        entries (subvec log next-index)]
    {:type :append-entries
     :term current-term
     :leader-id id
     :prev-log-index prev-log-index
     :prev-log-term prev-log-term
     :entries entries
     :leader-commit commit-index}))

(defn filter-match-index [key-set match-index]
  (into {} (filter (fn [[key val]]
                     (contains? key-set key))
                   match-index)))

(defn find-commit-index [match-index current-cluster new-cluster]
  (let [current-key-set (get-cluster-node-id-set current-cluster)
        current-match-index (filter-match-index current-key-set match-index)]
    (if (nil? new-cluster)
      (nth (sort (vals current-match-index)) (dec (:quorum current-cluster)))
      (let  [new-key-set (get-cluster-node-id-set new-cluster)
             new-match-index (filter-match-index new-key-set match-index)]
        (min (nth (sort (vals current-match-index)) (dec (:quorum current-cluster)))
             (nth (sort (vals new-match-index)) (dec (:quorum new-cluster))))))))

(defn handle-append-entries-response [message old-state config]
  (let [{:keys [id]} config
        {:keys [term success follower-id last-log-index]} message
        state (handle-message-term-bigger-than-current message old-state config)]
    (if (= (:role state) :leader)
      (let [{:keys [next-index match-index commit-index current-cluster new-cluster]} state]

        (if success
          (let [next-index (assoc next-index follower-id (+ 1 last-log-index))
                new-match-index (assoc match-index follower-id last-log-index)
                new-commit-index (find-commit-index match-index current-cluster new-cluster)
                new-state (assoc state
                                 :next-index next-index
                                 :match-index new-match-index
                                 :commit-index new-commit-index)]
            (struct handler-result
                    new-state
                    (if (not= new-commit-index commit-index)
                      (gen-heartbeat-message id new-state)
                      nil)
                    nil))
          (let [next-index-val (dec (get next-index follower-id 0))
                new-state (assoc state
                                 :next-index
                                 (assoc next-index
                                        follower-id
                                        (if (< next-index-val 0)
                                          0
                                          next-index-val)))]
            (struct handler-result
                    new-state
                    (gen-append-entries-message
                     id
                     state
                     (get (:next-index new-state) follower-id))))))
      (struct handler-result state nil nil))))
