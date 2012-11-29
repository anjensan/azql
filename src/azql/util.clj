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

(defmacro illegal-argument [& message]
  `(throw (IllegalArgumentException. (str ~@message))))

(defmacro illegal-state [& message]
  `(throw (IllegalStateException. (str ~@message))))

(defmacro check-argument
  [c & message]
  `(when (not ~c)
     (illegal-argument ~@message)))

(defmacro check-state
  [c & message]
  `(when (not ~c)
     (illegal-state ~@message)))

(defmacro check-type
  [val types & message]
  (let [vs (gensym)]
    `(let [~vs ~val]
       (check-argument
         (or ~@(map (fn [t] (list `instance? t vs)) types))
         ~@message))))

(def ^:private subquery-vars #{})

(defn register-subquery-var
  [s]
  "Adds var to 'subquery-vars'."
  (check-argument (var? s))
  (alter-var-root #'subquery-vars conj s))

(defn subquery-form?
  "Checks is form is 'subquery'. Should only be used in macros."
  ([ns form]
    (when (seq? form)
      (when-let [s (first form)]
        (contains? subquery-vars (resolve ns s)))))
  ([form]
    (subquery-form? *ns* form)))
