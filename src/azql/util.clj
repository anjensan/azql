(ns azql.util
  (:require [clojure.string :as s]))

(defn contains-element? 
  "True if seq contains element"
  [seq elm]
  (if (vector? seq)
    (reduce #(or %1 %2) (map #(= %1 elm) seq))
    (some #(= elm %) seq)))

(defn map-vals
  [f m]
  (into (empty m) (for [[k v] m] [k (f v)])))

(defn illegal-argument [& msg]
  (throw (IllegalArgumentException. (s/join msg))))

(defn illegal-state [& msg]
  (throw (IllegalStateException. (s/join msg))))

(defn todo []
  (throw (UnsupportedOperationException. "TODO")))
