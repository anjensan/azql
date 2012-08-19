(ns azql.core
  (:use [azql util expression emit render])
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

(defrecord Insert [table fields records]
  SqlLike
  (as-sql [this]
    (sql (render-insert this))))

(defrecord Delete [tables joins where]
  SqlLike
  (as-sql [this]
    (sql (render-delete this))))

(defrecord Update [table fields where]
  SqlLike
  (as-sql [this]
    (sql (render-update this))))

(defn select*
  "Creates empty select."
  []
  #azql.core.Select{})

(defmacro select
  "Creates new select."
  [& body]
  (emit-threaded-expression select* body))

(defn join*
  "Adds join section to query."
  [{:keys [tables joins] :as relation} type alias table cond]
  (let [a (as-alias alias)]
    (check-state (not (contains? table a)) (str "Relation already has table " a))
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
      `(join* ~relation ~join-key ~table ~table ~(prepare-macro-expression cond))))
 join-inner :inner, join :inner,
 join-right :right, join-left :left, join-full :full)

(defn fields*
  "Add fieldlist to query"
  [s fd]
  (check-argument (nil? (:fields s)) "Relation already has specified fields")
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

(defn order*
  "Adds 'order by' section to query"
  ([relation column] (order* relation column nil))
  ([{order :order :as relation} column dir]
     (check-argument
      (contains? #{:asc :desc nil} dir)
      (str "Invalid sort direction " dir))
     (assoc relation
       :order (cons [column dir] order))))

(defmacro order
  "Adds 'order by' section to query"
  ([relation column] `(order* ~relation ~(prepare-macro-expression column)))
  ([relation column dir] `(order* ~relation ~(prepare-macro-expression column) ~dir)))

(defn group
  "Adds 'group by' section to quiery"
  [{g :group :as relation} fields]
  (check-state (nil? g) (str "Relation already has grouping " g))
  (let [f (if (sequential? fields) fields [fields])]
    (assoc relation
      :group f)))

(defn modifier
  [{cm :modifier :as relation} m]
  (check-state (nil? cm) (str "Relation already has modifier " cm))
  (assoc relation :modifier m))

(defn limit
  "Limit number of rows"
  [{ov :limit :as relation} v]
  (check-argument (integer? v) "Limit must be integer")
  (check-state (nil? ov) (str "Relation already has limit " ov))
  (assoc relation :limit v))

(defn offset
  "Adds an offset to relation"
  [{ov :offset :as relation} v]
  (check-argument (integer? v) "Offset must be integer")
  (check-state (nil? ov) (str "Relation already has offset " ov))
  (assoc relation :offset v))

(defn having*
  "Adds 'having' condition to query"
  [{h :having :as s} c]
  (assoc s :having (conj-expression h c)))

(defmacro having
  "Adds 'having' condition to query, support macro expressions"
  [s c]
  `(having* ~s ~(prepare-macro-expression c)))

(defn- to-sql-params
  [relation]
  (let [{s :sql p :args} (sql relation)]
    (apply vector s p)))

(defmacro with-fetch
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
  (check-argument (>= 1 (count r)) "There is more than 1 record in resultset.")
  (first r))

(defn- single-result
  "Extract sinlge value from resultset. Useful for aggreagate functions."
  [r]
  (let [x (one-result r)]
    (when (not (nil? x))
      (check-state (== 1 (count r)) "There is more than 1 columns in record.")
      (val (first x)))))

(defn fetch-one
  "Executes query and return first element or throws exceptions
   if resultset contains more than one record"
  [relation]
  (jdbc/with-query-results* (to-sql-params relation) one-result))

(defn fetch-single
  "Executes quiery and return single result value. Useful for aggregate functions"
  [relation]
  (jdbc/with-query-results* (to-sql-params relation) single-result))

;; updates

(defn execute!
  "Executes update statement."
  [query]
  (let [{s :sql a :args} (sql query)]
    (first
     (jdbc/do-prepared s a))))

(defn- batch-arg?
  [v]
  (and (sequential? v) (:batch (meta v))))

(defn execute-return-keys!
  "Executes update statement and returns generated keys."
  [query]
  (let [{s :sql a :args} (sql query)]
    (check-argument
     (not-any? #(and (batch-arg? %) (not= (count %) 1)) a)
     "Can't return generated keys for batch query.")
    (jdbc/do-prepared-return-keys s (map #(if (batch-arg? %) (first %) %) a))))

(defn- prepare-batch-arguments
  [arguments]
  (if-let [ba (first (filter batch-arg? arguments))]
    (let [cnt (count ba)
          aseqs (map (fn [a]
                       (if-not (batch-arg? a)
                         (repeat cnt a)
                         (do
                           (check-state (= cnt (count a))
                                        "Batch argument vectors has different lengths.")
                           a)))
                     arguments)]
      (apply map vector aseqs))
    arguments))

(defn execute-batch!
  "Execute batch statement."
  [query]
  (let [{s :sql a :args} (sql query)]
    (apply jdbc/do-prepared s (prepare-batch-arguments a))))

(defn values
  "Add records to insert statement."
  [insert records]
  (let [records (if (map? records) [records] (seq records))]
    (assoc insert
      :records (into (:records insert) records))))

(defn delete*
  "Create new delete statement."
  ([] (->Delete nil nil nil))
  ([table] (from (delete*) table))
  ([alias table] (from (delete*) alias table)))

(defn insert*
  "Create new insert statement."
  ([table] (->Insert table nil []))
  ([table records] (values (insert* table) records)))

(defn update*
  "Create new update statement"
  ([table] (update* table table))
  ([alias table] (->Update [alias table] nil nil)))

(defn setf
  "Add field to update statement"
  [query fname value]
  (assoc query :fields (assoc (:fields query) fname value)))

(defmacro delete!
  "Delete records from a table."
  [& body]
  `(execute!
    ~(emit-threaded-expression delete* body)))

(defn execute-insert!
  "Execute insert quiery.
   If a single record is inserted, return map of the generated keys."
  [{r :records :as query}]
  (if (= 1 (count r))
    (execute-return-keys! query)
    (execute-batch! query)))

(defmacro insert!
  "Insert new record into a table.
   If a single record is inserted, return map of the generated keys."
  [& body]
  `(execute-insert!
    ~(emit-threaded-expression insert* body)))

(defmacro update!
  "Executes update statement"
  [& body]
  `(execute!
    ~(emit-threaded-expression update* body)))
