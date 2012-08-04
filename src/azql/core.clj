(ns azql.core
  (:use [azql util expression emit
         [render :only [render-select]]])
  (:use [clojure.set :only [difference]])
  (:use clojure.template)
  (:require [clojure.string :as s]
            [clojure.walk :as walk]
            [clojure.java.jdbc :as jdbc]))

(defrecord Select
    [tables joins fields where
     group having order
     modifier offset limit]
  SqlLike
  (as-sql [this] (sql (render-select this))))

(defn select*
  "Creates empty select."
  []
  #azql.core.Select{})

(defmacro select
  "Creates new select."
  [& body]
  (emit-threaded-expression select* body))

(defn- as-alias
  "Interprets value as column/table alias"
  [n]
  (keyword (name n)))

(defn join*
  "Adds join section to query."
  [{:keys [tables joins] :as relation} type alias table cond]
  (let [a (as-alias alias)]
    (when (contains? tables a)
      (illegal-argument "Relation already has table " a))
    (assoc relation
      :tables (assoc tables a table)
      :joins (conj (vec joins) [a type cond]))))

(do-template
 [join-name join-key]
 (defn join-name
   ([relation alias table] (join* relation join-key alias table nil))
   ([relation table] (join* relation join-key table table nil)))
 from nil, join-cross :cross)

(do-template
 [join-name join-key]
 (defmacro join-name
   ([relation alias table cond]
      `(join* ~relation ~join-key ~alias ~table ~(prepare-macro-expression cond)))
   ([relation table cond]
      `(join-left ~relation ~table ~table ~cond)))
 join-inner :inner, join :inner,
 join-right :right, join-left :left, join-full :full)

(defn fields*
  "Add fieldlist to query"
  [s fd]
  (when (:fields s)
    (illegal-argument "Relation already has specified fields"))
  (assoc s :fields fd))

(defn- prepare-fields
  [fs]
  (if (map? fs)
    (map-vals prepare-macro-expression fs)
    (into {} (map (juxt as-alias prepare-macro-expression) fs))))

(defmacro fields
  "Adds fieldlist to query, support macro expressions."
  [s fd]
  `(fields* ~s ~(prepare-fields fd)))

(defn where*
  "Adds 'where' condition to query"
  [{w :where :as s} c]
  (assoc s :where (conj-expression w c)))

(defmacro where
  "Adds 'where' condition to query, support macro expressions"
  [s c]
  `(where* ~s ~(prepare-macro-expression c)))

(defn order
  "Adds 'order by' section to query"
  ([relation column] (order relation column nil))
  ([{order :order :as relation} column dir]
     (when (not (contains? #{:asc :desc nil} dir))
       (illegal-argument "Invalig sort direction " dir))
     (assoc relation
       :order (conj (vec order) [column dir]))))

(defn modifier
  [{cm :modifier :as relation} m]
  (when cm
    (illegal-state "Relation already has modifier " cm))
  (assoc relation :modifier m))

(defn limit
  "Limit number of rows"
  [{ov :limit :as relation} v]
  (when (not (integer? v))
    (illegal-argument "Limit should be integer"))
  (when ov
    (illegal-state "Relation already has limit " ov))
  (assoc relation :limit v))

(defn offset
  "Adds an offset to relation"
  [{ov :offset :as relation} v]
  (when (not (integer? v))
    (illegal-argument "Offset should be integer"))
  (when ov
    (illegal-state "Relation already has offset " ov))
  (assoc relation :offset v))

(defn- to-sql-params
  [relation]
  (let [{s :sql p :args} (sql relation)]
    (apply vector s p)))

(defmacro with-results
  "Executes a query & evaluates body with 'v' bound to seq of results."
  [[v relation :as vr] & body]
  (assert (vector? vr))
  (assert (= 2 (count vr)))
  `(let [sp# (#'to-sql-params ~relation)]
     (jdbc/with-query-results ~v sp# ~@body)))

(defn fetch-all
  "Executes query and return results as vector"
  [relation]
  (jdbc/with-query-results* (to-sql-params relation) vec))

(defn- one-result
  "Extracts one record from resultset."
  [r]
  (when (< 1 (count r))
    (println (vec r))
    (throw (IllegalStateException. "There is more than 1 record in resultset.")))
  (first r))

(defn- single-result
  "Extract sinlge value from resultset. Useful for aggreagate functions."
  [r]
  (let [x (one-result r)]
    (when (not= 1 (count r))
      (throw (IllegalStateException. "There is more than 1 columns in record.")))
    (val (first x))))

(defn fetch-one
  "Executes query and return first element or throws exceptions
   if resultset contains more than one record"
  [relation]
  (jdbc/with-query-results* (to-sql-params relation) one-result))

(defn fetch-single
  "Executes quiery and return single result value. Useful for aggregate functions"
  [relation]
  (jdbc/with-query-results* (to-sql-params relation) single-result))
