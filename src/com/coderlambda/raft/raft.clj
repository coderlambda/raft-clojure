(ns com.coderlambda.raft.raft
  (:require [clojure.core.async :as async :refer [go <!]]
            [com.coderlambda.raft.common :refer :all]
            [com.coderlambda.raft.timers :refer :all]
            [com.coderlambda.raft.cluster :refer :all]
            [com.coderlambda.raft.election :refer :all]
            [com.coderlambda.raft.log-replicate :refer :all]))

(defn running? [state]
  (= (:running-state state) :running))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; control messages
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn handle-add-log [message old-state config]
  (let [{:keys [entries]} message
        {:keys [id]} config
        {:keys [leader role log]} old-state]
    (if (= role :leader)
      (let [next-index (count log)
            new-log (reduce conj log entries)
            new-state (assoc old-state
                             :log new-log)
            response (gen-append-entries-message id new-state next-index)]
        (struct handler-result new-state response nil))
      (struct handler-result old-state message leader))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; message handler factory
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-message-handler [message]
  (case (:type message)
    :stop (fn [message state config]
            (print-log "loop stoped" state)
            (struct handler-result (assoc state :running-state :stop) nil nil))
    :election-timeout handle-election-timeout
    :heartbeat-check handle-heartbeat-check
    :heartbeat handle-heartbeat
    :append-entries handle-append-entries 
    :append-entries-response handle-append-entries-response
    :vote-request handle-vote-request
    :vote-request-response handle-vote-request-response
    :add-log handle-add-log
    (fn [message state config] 
      (print "un handled message" message)
      (struct handler-result state nil nil))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; raft main
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn start-init-timers [state channel]
  (case (:role state)
    :follower (set-heartbeat-check-timer channel (:heartbeat-timeout state))
    :candidate (set-election-timer channel (:current-term state) (gen-timeout))
    :leader (set-heartbeat-timer channel (:heartbeat-interval state))))

(defn reset-timers [is-heartbeat? old-state new-state channel]
  (case (:role new-state)
    :follower (if (not= (:current-term old-state) (:current-term new-state))
                (set-heartbeat-check-timer channel (:heartbeat-timeout new-state)))
    :candidate (if (not= (:current-term old-state) (:current-term new-state))
                (set-election-timer channel (gen-timeout) (:current-term new-state)))
    :leader (if (or is-heartbeat?
                    (= (:role old-state) :candidate))
              (set-heartbeat-timer channel (:heartbeat-interval new-state)))))

(defn raft [init-config state]
  (let [{:keys [id channel]} init-config
        {:keys [heartbeat-timeout]} state] 
    (start-init-timers state channel)
    (go (loop [old-state state
               config init-config]
          (when (= (:running-state old-state) :running)
            (let [message (<! channel)
                  handler (get-message-handler message)
                  is-heartbeat? (= (:type message) :heartbeat)
                  {:keys [new-state response target]} (handler message old-state config)
                  {:keys [current-cluster new-cluster]} new-state]
              (reset-timers is-heartbeat? old-state new-state channel)
              (if response
                (if target
                  (send-message response target current-cluster new-cluster)
                  (broadcast-message response current-cluster new-cluster)))
              (recur new-state config)))))))


