(ns azql.core
  (:use [azql util expression emit])
  (:use [clojure.set :only [difference]])
  (:use clojure.template)
  (:require [clojure.string :as s]
            [clojure.walk :as walk]))

(declare render-select)

(defrecord Select
    [tables joins fields where group having]
  SqlLike
  (as-sql [this] (render-select this)))

(defrecord Table
    [name]
  SqlLike
  (as-sql [this]
    (sql SELECT ASTERISK FROM (qname name))))

(defn- as-alias
  "Interprets value as column/table alias"
  [n]
  (keyword (name n)))

(defn as-table-or-subquery
  "Converts value to table name or subquery.
   Surrounds subquery into parenthesis."
  [v]
  (cond
   (keyword? v) v
   (string? v) (keyword v)
   (instance? Table v) (keyword (:name v))
   :else (parenthesis v)))

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
 [jname jkey]
 (defn jname
   ([relation alias table] (join* relation jkey alias table nil))
   ([relation table] (join* relation jkey table table nil)))
 from nil, join-cross :cross)

(do-template
 [jname jkey]
 (defmacro jname
   ([relation alias table cond]
      `(join* ~relation ~jkey ~alias ~table ~(prepare-macro-expression cond)))
   ([relation table cond]
      `(join-left ~relation ~table ~table ~cond)))
 join-inner :inner, join :inner,
 join-right :right, join-left :left, join-full :full)

(defn select*
  "Creates empty select."
  [] #azql.core.Select{})

(defmacro select
  "Creates new select."
  [& body]
  (emit-threaded-expression select* body))

(defn- prepare-fields
  [fs]
  (if (map? fs)
    (map-vals prepare-macro-expression fs)
    (into {} (map (juxt as-alias prepare-macro-expression) fs))))

(defn fields*
  "Add fieldlist to query"
  [s fd]
  (when (:fields s)
    (illegal-argument "Relation already has specified fields"))
  (assoc s :fields fd))

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

(defn- render-from-table
  [alias nm]
  (let [t (as-table-or-subquery nm)]
    (if (= alias t) t [t alias])))

(defn- render-field
  [alias nm]
  (if (= alias nm) nm [(render-expression nm) alias]))

(defn- render-fields-section
  [fields tables]
  (if (or (nil? fields) (= fields :*))
    ASTERISK
    (interpose COMMA (map (fn [[a b]] (render-field a b)) fields))))

(defn- join-type
  [jt]
  (get
   {:left LEFT_OUTER_JOIN, :right RIGHT_OUTER_JOIN,
    :full FULL_OUTER_JOIN, :inner INNER_JOIN, :cross CROSS_JOIN}
   jt jt))

(defn- render-from-section
  [tables joins]
  [(let [[a jn] (first joins)
         t (tables a)]
     (when-not (contains? #{nil :cross} jn)
       (illegal-state "First join should be CROSS JOIN"))
     (render-from-table a t))
   (for [[a jn c] (rest joins) :let [t (tables a)]]
     (if (nil? jn)
       [COMMA (render-from-table a t)]
       [(join-type jn)
        (render-from-table a t)
        (if c [ON (render-expression c)] NONE)]))])

(defn- render-where-section
  [where]
  (render-expression where))

(defn- render-select
  [{:keys [fields tables joins where] :as relation}]
  (as-sql
   [SELECT (render-fields-section fields tables)
    FROM (render-from-section tables joins)
    (if where [WHERE (render-where-section where)] NONE)]))


