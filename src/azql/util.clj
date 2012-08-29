(ns azql.util
  (:require [clojure.string :as s]))

(defn map-vals
  "Apply f to the values of map m. Always returns new map.
   Returns empty hash map if `m` is nil."
  [f m]
  (into (if (map? m) (empty m) {}) (for [[k v] m] [k (f v)])))

(defn code-form?
  [f]
  (or
   (instance? clojure.lang.Cons f)
   (list? f)))

(defn emit-threaded-expression
  "Executes 'sfun' with first constant values from 'body' as arguments
   and threads result through the rest of 'body' with '->'"
  [sfun body]
  (let [[s b] (split-with (complement code-form?) body)]
    `(-> (~sfun ~@s) ~@b)))

(defn eager-filtered-flatten
  "Eager version of `flatten`.
   Second argument is a test function.
   If it returns true, then collection will be flattened."
  ([col flat?]
     (letfn [(rec [acc item]
               (if (and (sequential? item) (flat? item))
                 (reduce rec acc item)
                 (conj! acc item)))]
       (persistent! (rec (transient []) [col]))))
  ([col] (eager-filtered-flatten col (constantly true))))

(defn illegal-argument [& msg]
  (throw (IllegalArgumentException. (s/join msg))))

(defn illegal-state [& msg]
  (throw (IllegalStateException. (s/join msg))))

(defmacro check-argument
  [c msg]
  `(when (not ~c)
     (illegal-argument ~msg)))

(defmacro check-state
  [c msg]
  `(when (not ~c)
     (illegal-state ~msg)))
