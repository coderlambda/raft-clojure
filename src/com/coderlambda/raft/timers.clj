(ns com.coderlambda.raft.timers
  (:require [clojure.core.async :as async :refer [timeout go <! >!]]))

(defn gen-timeout []
  (+ 300 (rand-int 150)))

(defn set-election-timer [channel interval term]
  (go (<! (timeout interval))
      (>! channel
          {:type :election-timeout
           :term term})))

(defn set-heartbeat-check-timer [channel interval]
  (go (<! (timeout interval))
      (>! channel
          {:type :heartbeat-check})))

(defn set-heartbeat-timer [channel interval]
  (go (<! (timeout interval))
      (>! channel
          {:type :heartbeat})))
