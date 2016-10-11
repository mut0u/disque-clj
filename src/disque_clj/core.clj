(ns disque-clj.core
  (:require [clojure.string :as str]
            [taoensso.carmine :as car]))


(def flags
  {:async true :nohang true :withcounters true})

(defn- is-flag? [v]
  ((first v) flags))

(defn- concat-opts [base opts]
  (reduce #(if (and (last %2) (is-flag? %2)) (conj %1 (first %2)) (concat %1 %2)) base (seq opts)))

(defn info []
  (car/redis-call [:info]))

(defn debug-flushall []
  (car/redis-call [:debug "FLUSHALL"]))

(defn ping []
  (car/redis-call [:ping]))

(defn addjob
  ([queue job timeout]
   (car/redis-call [:addjob queue job timeout]))
  ([queue job timeout opts]
   (car/redis-call (concat-opts [:addjob queue job timeout] opts))))

(defn getjob
  ([queues]
   (car/redis-call (concat [:getjob :from] queues)))
  ([queues opts]
   (car/redis-call (concat (concat-opts [:getjob] opts) [:from] queues))))

(defn deljob [ids]
  (car/redis-call (concat [:DELJOB] ids)))

(defn ackjob [ids]
  (car/redis-call (concat [:ackjob] ids)))

(defn fastack [ids]
  (car/redis-call (concat [:fastack] ids)))

(defn nack [ids]
  (car/redis-call (concat [:nack] ids)))

(defn working [id]
  (car/redis-call [:working id]))

(defn show [id]
  (car/redis-call [:show id]))

(defn qlen [queue]
  (car/redis-call [:qlen queue]))

(defn qpeek [queue n]
  (car/redis-call [:qpeek queue n]))
