(ns com.coderlambda.raft.raft
  (:require [clojure.core.async :as async :refer [chan timeout go go-loop <! >!]]))

(defn print-log [& args]
  (locking *out*
    (apply println args)))


(defn running? [state]
  (= (:running-state state) :running))

(defstruct cluster-info
  :id
  :channel)

(defstruct config 
  :id
  :channel
  :cluster-info
  :quorum)

(defstruct handler-result
  :new-state
  :response
  :target)

(defstruct log-entry
  :term
  :command)

(def init-state {:running-state :running
                 :role :follower
                 :leader nil
                 :current-term 0
                 ;; election states
                 :granted-vote-count 0
                 :vote-for nil
                 ;; log
                 :log []
                 ;; volatile states
                 :commit-index -1
                 :last-applied -1
                 ;; volatile leader states
                 :next-index {}
                 :match-index {}
                 ;; timer control states
                 :heartbeat-timestamp 0
                 :heartbeat-timeout 300
                 :heartbeat-interval 250})

(defn broadcast-message [message config]
  (doseq [target (:cluster-info config)]
    (go (>! (:channel target) message))))

(defn send-message [message target config]
  (doseq [node (:cluster-info config)]
    (if (= (:id node) target)
      (go (>! (:channel node) message)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; timers
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn gen-timeout []
  (+ 300 (rand-int 150)))

(defn set-election-timer [term config t]
  (go (<! (timeout t))
      (>! (:channel config)
          {:type :election-timeout
           :term term})))

(defn set-heartbeat-check-timer [config time]
  (go (<! (timeout time))
      (>! (:channel config)
          {:type :heartbeat-check})))

(defn set-heartbeat-timer [config state]
  (go (<! (timeout (:heartbeat-interval state)))
      (>! (:channel config)
          {:type :heartbeat})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; role change functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn become-follower [state config]
  (print-log (:id config) "become follower")
  (assoc state
         :role :follower
         :granted-vote-count 0
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
           :current-term new-term)))

(defn become-leader [state config]
  (let [{:keys [id cluster-info]} config
        next-index (count (:log state))]
    (print-log (:id config) "become leader")
    (assoc state
           :role :leader
           :leader id
           :granted-vote-count 0
           :next-index (reduce #(assoc %1 (:id %2) next-index) {} cluster-info)
           :match-index (reduce #(assoc %1 (:id %2) 0) {} cluster-info))))

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
        {:keys [id quorum]} config
        {:keys [role current-term granted-vote-count]} old-state
        new-state (if (and (= term current-term) (= role :candidate))
                   (if (>= granted-vote-count quorum)
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
        response (if (= role :leader)
                   (gen-heartbeat-message id old-state))]
    (struct handler-result old-state response nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; rpc handlers
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn handle-message-term-bigger-than-current [message old-state config]
  (if (> (:term message) (:current-term old-state))
    (become-follower (assoc old-state :current-term (:term message)) config)
    old-state))

(defn handle-vote-request [message old-state config]
  (let [{:keys [id]} config
        {:keys [term candidate-id last-log-index last-log-term]} message
        state (handle-message-term-bigger-than-current message old-state config)
        {:keys [heartbeat-timeout current-term vote-for log]} state
        vote-granted? (if (and (not vote-for)
                               (= term current-term))
                        (let [last-log (last log)]
                          (if (and last-log last-log-term)
                            (or (> last-log-term (:term last-log))
                                (and (= last-log-term (:term last-log))
                                     (>= last-log-index (dec (count log)))))
                            (or (and (nil? last-log)
                                     (nil? last-log-term))
                                (not (nil? last-log-term))))))
        response {:type :vote-request-response
                  :term current-term
                  :vote-granted vote-granted?}
        new-state (if vote-granted?
                    (assoc state 
                           :vote-for candidate-id
                           :current-term term
                           :heartbeat-timestamp (System/currentTimeMillis))
                    state)]
    (if (:vote-granted response)
      (print-log id "vote for" candidate-id)
      (print-log id "current term is" current-term 
               ", has vote for" vote-for
               ", refuse the vote request from" (:candidate-id message) 
               "with term" (:term message)))
    (struct handler-result new-state response candidate-id)))

(defn handle-vote-request-response [message old-state config]
  (let [{:keys [term vote-granted]} message
        {:keys [id quorum]} config
        state (handle-message-term-bigger-than-current message old-state config)
        new-state (if (and (= (:role state) :candidate)
                           (= (:current-term state) term)
                           vote-granted)
                    (let [new-granted-vote-count (+ (:granted-vote-count state) 1)]
                      (if (>= new-granted-vote-count quorum)
                        (become-leader state config)
                        (assoc state 
                               :granted-vote-count new-granted-vote-count)))
                    state)]
    (struct handler-result new-state nil nil)))

(defn gen-append-entries-response-message [id state success]
  (let [{:keys [current-term log]} state]
    {:type :append-entries-response
     :follower-id id
     :term current-term
     :success success
     :last-log-index (dec (count log))}))

(defn handle-append-entries [message old-state config]
  (let [{:keys [id]} config
        {:keys [term leader-id prev-log-index prev-log-term entries leader-commit]} message
        {:keys [current-term leader log commit-index]} old-state
        state (if (> term current-term)
                (assoc (become-follower (assoc old-state :current-term term) config)
                       :leader leader-id)
                (assoc old-state 
                       :leader leader-id
                       :heartbeat-timestamp (System/currentTimeMillis)))
        new-state (cond 
                    (not prev-log-index)
                    (assoc state
                           :log entries
                           :commit-index -1
                           :last-applied -1)
                    (and (< prev-log-index (count log))
                         (= prev-log-term (:term (nth log prev-log-index))))
                    (let [log (reduce conj (subvec log 0 (inc prev-log-index)) entries)
                          new-commit-index (min leader-commit (dec (count log)))]
                      (assoc state
                             :log log
                             :commit-index new-commit-index))
                        
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

(defn find-commit-index [match-index quorum]
  (nth (sort (vals match-index)) (dec quorum)))

(defn handle-append-entries-response [message old-state config]
  (let [{:keys [id quorum]} config
        {:keys [term success follower-id last-log-index]} message
        state (handle-message-term-bigger-than-current message old-state config)]
    (if (= (:role state) :leader)
      (let [{:keys [next-index match-index commit-index]} state]
        (if success
          (let [mext-index (assoc next-index follower-id last-log-index)
                match-index (assoc match-index follower-id last-log-index)
                new-commit-index (find-commit-index match-index quorum)
                new-state (assoc state
                         :next-index next-index
                         :match-index match-index
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
(defn start-init-timers [state config]
  (case (:role state)
    :follower (set-heartbeat-check-timer config (:heartbeat-timeout state))
    :candidate (set-election-timer (:current-term state) config (gen-timeout))
    :leader (set-heartbeat-timer config state)))

(defn reset-timers [is-heartbeat? old-state new-state config]
  (case (:role new-state)
    :follower (if (not= (:current-term old-state) (:current-term new-state))
                (set-heartbeat-check-timer config (:heartbeat-timeout new-state)))
    :candidate (if (not= (:current-term old-state) (:current-term new-state))
                (set-election-timer (:current-term new-state) config (gen-timeout)))
    :leader (if (or is-heartbeat?
                    (= (:role old-state) :candidate))
              (set-heartbeat-timer config new-state))))

(defn raft [config state]
  (let [{:keys [id channel id]} config
        {:keys [heartbeat-timeout]} state] 
    (start-init-timers state config)
    (go (loop [old-state state]
          (when (= (:running-state old-state) :running)
            (let [message (<! channel)
                  handler (get-message-handler message)
                  is-heartbeat? (= (:type message) :heartbeat)
                  {:keys [new-state response target]} (handler message old-state config)]
              (reset-timers is-heartbeat? old-state new-state config)
              (if response
                (if target
                  (send-message response target config)
                  (broadcast-message response config)))
              (recur new-state)))))))


