(ns azql.util
  (:require [clojure.string :as s]))

(defn map-vals
  "Apply f to the values of map m. Always returns new map.
   Returns empty hash map if `m` is nil."
  [f m]
  (into (if (map? m) (empty m) {}) (for [[k v] m] [k (f v)])))

(defn emit-threaded-expression
  "Executes 'sfun' with first constant values from 'body' as arguments
   and threads result through the rest of 'body' with '->'"
  [sfun body]
  (let [[s b] (split-with (complement seq?) body)]
    `(-> (~sfun ~@s) ~@b)))

(definline keyword-or-string?
  [v]
  `(let [v# ~v] (or (keyword? v#) (string? v#))))

(defmacro illegal-argument [& msg]
  `(throw (IllegalArgumentException. (str ~@msg))))

(defmacro illegal-state [& msg]
  `(throw (IllegalStateException. (str ~@msg))))

(defmacro check-argument
  [c msg]
  `(when (not ~c)
     (illegal-argument ~msg)))

(defmacro check-state
  [c msg]
  `(when (not ~c)
     (illegal-state ~msg)))

(defmacro check-type
  [val types & message]
  (let [vs (gensym)]
    `(let [~vs ~val]
       (check-argument
         (or ~@(map (fn [t] (list `instance? t vs)) types))
         ~@message))))

(def ^:private subquery-symbols #{})

(defn register-subquery-symbol
  [s]
  "Adds symbol to `subquery-symbols`."
  (check-argument (not (namespace s)))
  (alter-var-root #'subquery-symbols conj (symbol s)))

(defn subquery-form?
  "Checks is form is 'subquery'."
  [form]
  (when (seq? form)
    (let [f (first form)]
      (contains? subquery-symbols f))))
