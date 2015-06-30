(ns com.coderlambda.raft.common)

(defn print-log [& args]
  (locking *out*
    (apply println args)))

(defstruct node-info
  :id
  :channel)

(defstruct cluster-info
  :node-map
  :quorum)

(defstruct handler-result
  :new-state
  :response
  :target)

(defstruct log-entry
  :term
  :command)

(def default-config {:id "default"
                     :channel nil
                     :current-cluster '()
                     :new-cluster '()})

(def init-state {:running-state :running
                 :role :follower
                 :leader nil
                 :current-term 0
                 ;; election states
                 :granted-vote-count 0
                 :voted-nodes #{}
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

