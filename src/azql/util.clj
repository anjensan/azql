(ns azql.util
  (:require [clojure.string :as s]))

(defn map-vals
  "Apply f to the values of map m. Returns new map"
  [f m]
  (into (empty m) (for [[k v] m] [k (f v)])))

(defn illegal-argument [& msg]
  (throw (IllegalArgumentException. (s/join msg))))

(defn illegal-state [& msg]
  (throw (IllegalStateException. (s/join msg))))

(defn todo []
  (throw (UnsupportedOperationException. "TODO")))
