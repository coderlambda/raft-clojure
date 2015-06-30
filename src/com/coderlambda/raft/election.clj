(ns com.coderlambda.raft.election
  (:require [com.coderlambda.raft.common :refer [print-log handler-result]]
            [com.coderlambda.raft.cluster :refer [broadcast-message
                                                  send-message
                                                  majority-in-both?
                                                  get-cluster-node-id-set
                                                  combine-cluster-nodes]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; role change functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn become-follower [state config]
  (print-log (:id config) "become follower")
  (assoc state
         :role :follower
         :voted-nodes #{}
         :vote-for nil
         :heartbeat-timestamp (System/currentTimeMillis)
         :leader nil))

(defn become-candidate [state config]
  (let [{:keys [current-term]} state
        {:keys [id cluster-info]} config
        new-term (+ current-term 1)]
    (print-log (:id config) "become candadite at term" new-term)
    (assoc state
           :role :candidate
           :leader nil
           :vote-for nil
           :voted-nodes #{}
           :current-term new-term)))

(defn become-leader [state config]
  (let [{:keys [id]} config
        {:keys [current-cluster new-cluster]} state
        next-index (count (:log state))
        all-node-ids (into #{} (map #(:id %1) (combine-cluster-nodes current-cluster new-cluster)))]
    (print-log (:id config) "become leader")

    (assoc state
           :role :leader
           :leader id
           :voted-nodes #{}
           :next-index (reduce #(assoc %1 %2 next-index) {} all-node-ids)
           :match-index (reduce #(assoc %1 %2 0) {} all-node-ids))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; timeout handlers
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn gen-vote-request [id state]
  (let [{:keys [current-term log]} state
        last-log (last log)]
    {:type :vote-request
     :term current-term
     :candidate-id id
     :last-log-index (if last-log (dec (count log)))
     :last-log-term (if last-log (:term last-log))}))

(defn gen-heartbeat-message [id state]
  (let [{:keys [current-term log commit-index]} state
        last-log (last log)]
    {:type :append-entries
     :term current-term
     :leader-id id
     :prev-log-index (if last-log (dec (count log)))
     :prev-log-term (if last-log (:term last-log))
     :entries []
     :leader-commit commit-index}))

(defn handle-election-timeout [message old-state config]
  (let [term (:term message)
        {:keys [id]} config
        {:keys [role current-term voted-nodes current-cluster new-cluster]} old-state
        new-state (if (and (= term current-term) (= role :candidate))
                    (if (majority-in-both? voted-nodes current-cluster new-cluster)
                      (become-leader old-state config)
                      (do (print-log id "election time out")
                          (become-candidate old-state config)))
                    old-state)
        response  (if (and (= term current-term) (= role :candidate))
                    (case (:role new-state)
                      :candidate (gen-vote-request id new-state)
                      :leader (gen-heartbeat-message id new-state)
                      :else nil))]
    (struct handler-result new-state response nil)))

(defn handle-heartbeat-check [message state config]
  (let [{:keys [id]} config
        {:keys [heartbeat-timeout heartbeat-timestamp role current-term]} state
        current-time (System/currentTimeMillis)
        timeout? (<  (- (+ heartbeat-timestamp heartbeat-timeout) current-time) 0)
        new-state (if timeout?
                    (do (print-log id "heartbeat timeout")
                        (become-candidate state config))
                    state)
        response (if timeout?
                   (gen-vote-request id new-state))]
    (struct handler-result new-state response nil)))

(defn handle-heartbeat [message old-state config]
  (let [{:keys [role current-term]} old-state
        {:keys [id]} config
        response (if (= role :leader) ;; for other roles, stop heartbeat
                   (gen-heartbeat-message id old-state))]
    (struct handler-result old-state response nil)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; rpc handlers
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn handle-message-term-bigger-than-current [message old-state config]
  (if (> (:term message) (:current-term old-state))
    (become-follower (assoc old-state :current-term (:term message)) config)
    old-state))

(defn vote-granted? [{:keys [term last-log-index last-log-term]}
                     {:keys [current-term vote-for log]}]
  (if (and (not vote-for)
           (>= term current-term))
    (let [last-log (last log)]
      (if (and last-log last-log-term)
        (or (> last-log-term (:term last-log))
            (and (= last-log-term (:term last-log))
                 (>= last-log-index (dec (count log)))))
        (or (and (nil? last-log)
                 (nil? last-log-term))
            (not (nil? last-log-term)))))))

(defn handle-vote-request [message old-state config]
  (let [{:keys [id]} config
        {:keys [term candidate-id]} message
        state (handle-message-term-bigger-than-current message old-state config)
        {:keys [heartbeat-timeout current-term vote-for]} state
        granted? (vote-granted? message state)
        response {:type :vote-request-response
                  :term current-term
                  :node-id id
                  :vote-granted granted?}
        new-state (if granted?
                    (assoc state 
                           :vote-for candidate-id
                           :current-term term
                           :heartbeat-timestamp (System/currentTimeMillis))
                    state)]
    (if (:vote-granted response)
      (print-log id "vote for" candidate-id)
      (print-log id "current term is" current-term 
                 ", has vote for" vote-for
                 ", refuse the vote request from" candidate-id
                 "with term" term))
    (struct handler-result new-state response candidate-id)))

(defn handle-vote-request-response [message old-state config]
  (let [{:keys [term vote-granted node-id]} message
        {:keys [id]} config
        state (handle-message-term-bigger-than-current message old-state config)
        new-state (let [{:keys [role current-term voted-nodes current-cluster new-cluster]} state]
                    (if (and (= role :candidate)
                             (= current-term term)
                             vote-granted)
                      (let [new-voted-nodes (conj voted-nodes node-id)]
                        (if (majority-in-both? new-voted-nodes current-cluster new-cluster)
                          (become-leader state config)
                          (assoc state 
                                 :voted-nodes new-voted-nodes)))
                      state))]
    (struct handler-result new-state nil nil)))
