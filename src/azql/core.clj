(ns azql.core
  (:use [azql util dialect connection emit expression render])
  (:use clojure.template)
  (:require [clojure.string :as s])
  (:require [clojure.java.jdbc :as jdbc]))

(defmacro with-azql-context
  "Executes code in azql context (global connection, naming strategy ect.).
   All public functions automatically call this macro."
  [& body]
  `(-> (do ~@body)
     with-dialect-naming-strategy
     with-recognized-dialect
     with-global-connection))

(defn sql
  "Converts object to Sql. Requires jdbc-coneection."
  ([& args]
    (with-azql-context (apply sql* args))))

(defrecord Select
  [tables joins fields where
   group having order
   modifier offset limit]
  Query
  SqlLike
  (as-sql [this] (as-sql (render-select this))))

(defrecord Insert [table fields records]
  Query
  SqlLike
  (as-sql [this] (as-sql (render-insert this))))

(defrecord Delete [tables joins where]
  Query
  SqlLike
  (as-sql [this] (as-sql (render-delete this))))

(defrecord Update [table fields where]
  Query
  SqlLike
  (as-sql [this] (as-sql (render-update this))))

(defrecord CombinedQuery
  [type queries order limit offset modifier]
  Query
  SqlLike
  (as-sql [this] (as-sql (render-combined this))))

(defrecord PrecompiledSelect [content]
  Query
  SqlLike
  (as-sql [this] content))

(do-template
  [v]
  (defmethod print-method v [s ^Appendable w]
    (.append w (interpolate-sql s)))
  Select Insert Update Delete CombinedQuery)

(defmethod print-method PrecompiledSelect [^PrecompiledSelect s ^Appendable w]
  (.append w (interpolate-sql (.content s))))

(declare fields*)
(declare from)

(defn select*
  "Creates empty select."
  ([] #azql.core.Select{})
  ([fields]
    (-> (select*) (fields* fields))))

(defmacro select
  "Creates new select."
  [a & body]
  (emit-threaded-expression
    `select*
    (list* (if (list? a) a `(fields ~a)) body)))

(defrecord SurrogatedArg [symbol]
  SqlLike
  (as-sql [this] (arg this)))

(defn- emit-precompiled-select
  [name args body]
  (let [sargs (mapv ->SurrogatedArg args)
        sargs-args-map (into {} (map vector sargs args))]
    `(let [sqls# (atom {})
           original# (fn ~args (select ~@body (sql)))
           compile# (fn [] (apply original# ~sargs))]
       (defn ~name ~args
         (let [dialect# (current-dialect)
               cached-sql# (get @sqls# dialect#)
               sql# (if cached-sql#
                      cached-sql#
                      (let [new-sql# (compile#)]
                        (swap! sqls# assoc dialect# new-sql#)
                        new-sql#))
               args# (:args sql#)]
           (->PrecompiledSelect
             (assoc sql# :args (replace ~sargs-args-map args#))))))))

(defn- emit-raw-select
  [name args sql]
  (let [args-map (into {} (map (juxt keyword identity) args))]
    `(defn ~name ~args
       (->PrecompiledSelect
         (format-sql ~sql ~args-map)))))

(defmacro defselect
  "Define new pre-rendered query.
   Query compiled once for each dialect."
  [& name-args-body]
  (let [[name args & body]
        (if (string? (second name-args-body))
          (list*
            (vary-meta (first name-args-body) assoc :doc (second name-args-body))
            (nnext name-args-body))
          name-args-body)]
    (check-argument
      (symbol? name)
      "First argument to defselect should be a Symbol")
    (check-argument
      (every? symbol? args)
      "Macro defselect doesn't support destructuring")
    (check-argument
      (vector? args)
      "Parameter declaration quote should be a vector")
    (if (and (== 1 (count body)) (string? (first body)))
      (emit-raw-select name args (first body))
      (emit-precompiled-select name args body))))

(defn table
  "Select all records from a table."
  [tname]
  (vary-meta (select (from tname)) assoc ::created-by-table-fn true))

(defn- single-table-select?
  [q]
  (when (and
          (::created-by-table-fn  (meta q))
          (== 1 (count (:tables q))))
    (let [[a t] (first (:tables q))]
      (and
        (keyword-or-string? t)
        (= (name a) (name t))
        (=
          #{:tables :joins}
          (set (map key (filter #(-> % val nil? not) q))))))))

(defn- unwrap-single-table
  [q]
  (if (single-table-select? q)
    (val (first (:tables q)))
    q))

(defn join*
  "Adds join section to query."
  [{:keys [tables joins] :as query} type alias table cond]
  (check-type query [Select Delete] "Firt argument must be a Query")
  (let [t (unwrap-single-table table)
        a (as-alias (or alias t))]
    (check-state (not (contains? tables a)) (str "Relation already has table " a))
    (assoc
      query
      :tables (assoc tables a t)
      :joins (conj (or joins []) [a type cond]))))

(do-template
 [join-name join-key]
 (defn join-name
   ([relation alias table] (join* relation join-key alias table nil))
   ([relation table] (join* relation join-key nil table nil)))
 from nil, join-cross :cross)

(do-template
  [join-name join-key]
  (defmacro join-name
   ([relation alias table cond]
     `(join* ~relation ~join-key ~alias ~table ~(prepare-macro-expression cond &env)))
   ([relation table cond]
     `(let [table# ~table]
        (join* ~relation ~join-key nil table# ~(prepare-macro-expression cond &env)))))
  join-inner :inner, join :inner,
  join-right :right, join-left :left, join-full :full)

(defn fields*
  "Adds field list to query."
  [query fd]
  (check-type query [Select Insert] "Firt argument must be a Query")
  (check-argument (nil? (:fields query)) "Relation already has specified fields.")
  (assoc query :fields fd))

(defn- prepare-fields
  [fs env]
  (if (map? fs)
    (map-vals #(prepare-macro-expression % env) fs)
    (let [aa (if (= 1 (count fs))
               as-alias
               as-alias-safe)]
      (into {} (map (juxt aa #(prepare-macro-expression % env)) fs)))))

(defmacro fields
  "Adds field list to query, support expressions."
  [query fd]
  `(fields* ~query ~(prepare-fields fd &env)))

(defn where*
  "Adds 'where' condition to query"
  [{w :where :as query} c]
  (check-type query [Select Delete Update] "Firt argument must be a Query")
  (assoc query :where (conj-expression w c)))

(defmacro where
  "Adds 'where' condition to query, support expressions."
  [s c]
  `(where* ~s ~(prepare-macro-expression c &env)))

(defn order*
  "Adds 'order by' section to query."
  ([relation column] (order* relation column nil))
  ([{order :order :as relation} column dir]
    (check-type relation [Select CombinedQuery] "Firt argument must be a Select")
    (check-argument
      (contains? #{:asc :desc nil} dir)
      (str "Invalid sort direction " dir))
    (assoc
      relation
      :order (cons [column dir] order))))

(defmacro order
  "Adds 'order by' section to query."
  ([relation column] `(order* ~relation ~(prepare-macro-expression column &env)))
  ([relation column dir] `(order* ~relation ~(prepare-macro-expression column &env) ~dir)))

(defn group
  "Adds 'group by' section to query."
  [{g :group :as relation} fields]
  (check-type relation [Select] "Firt argument must be a Select")
  (check-argument (nil? g) (str "Relation already has grouping " g))
  (let [f (if (sequential? fields) fields [fields])]
    (assoc relation :group f)))

(defn modifier
  "Attaches a modifier to the query. Modifier should be keyword or raw sql."
  [{cm :modifier :as relation} m]
  (check-type relation [Select CombinedQuery] "Firt argument must be a Select")
  (check-argument (nil? cm) (str "Relation already has modifier " cm))
  (check-argument (or (sql? m) (#{:distinct :all} m))
                  "Invalid modifier, expected :distinct, :all or raw sql.")
  (assoc relation :modifier m))

(defn limit
  "Limits number of rows."
  [{ov :limit :as relation} v]
  (check-type relation [Select CombinedQuery] "Firt argument must be a Select")
  (check-state (nil? ov) (str "Relation already has limit " ov))
  (assoc relation :limit v))

(defn offset
  "Adds an offset to query."
  [{ov :offset :as relation} v]
  (check-type relation [Select CombinedQuery] "Firt argument must be a Select")
  (check-state (nil? ov) (str "Relation already has offset " ov))
  (assoc relation :offset v))

(defn having*
  "Adds 'having' condition to query."
  [{h :having :as relation} c]
  (check-type relation [Select] "Firt argument must be a Select")
  (assoc relation :having (conj-expression h c)))

(defmacro having
  "Adds 'having' condition to query, supports macro expressions"
  [s c]
  `(having* ~s ~(prepare-macro-expression c &env)))

(defn add-query
  "Adds new select to combined query."
  [combined query]
  (check-type combined [CombinedQuery] "Firt argument must be a CombinedQuery")
  (assoc combined :queries (conj (or (:queries combined) []) query)))

(defn combine*
  [type]
  (assoc #azql.core.CombinedQuery{} :type type))

(defmacro combine
  "Creates new select combined query."
  [type & body]
  (emit-threaded-expression
    `combine*
    (cons
      (keyword type)
      (map (fn [f] (if (subquery-form? f &env) `(add-query ~f) f)) body))))

(defmacro union
  "Creates union between queries."
  [& body]
  (list* `combine :union body))

(defmacro intersect
  "Creates intersection between queries."
  [& body]
  (list* `combine :intersect body))

(defmacro except
  "Creates union between queries."
  [& body]
  (list* `combine :except body))

;; fetching

(defn- to-sql-params
  [relation]
  (let [{s :sql p :args} (sql* relation)]
    (apply vector s p)))

(defmacro with-fetch
  "Executes a query & evaluates body with 'v' bound to seq of results."
  [[v relation :as vr] & body]
  (check-argument (vector? vr))
  (check-argument (= 2 (count vr)))
  `(with-azql-context
     (let [sp# (#'to-sql-params ~relation)]
       (jdbc/with-query-results ~v sp# ~@body))))

(defn fetch-all
  "Executes query and returns results as a vector."
  [relation]
  (with-azql-context
    (jdbc/with-query-results* (to-sql-params relation) vec)))

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
  "Executes query and returns first element or throws an exception
   if resultset contains more than one record."
  [relation]
  (with-azql-context
    (jdbc/with-query-results* (to-sql-params relation) one-result)))

(defn fetch-single
  "Executes quiery and return single result value. Useful for aggregate functions."
  [relation]
  (with-azql-context
    (jdbc/with-query-results* (to-sql-params relation) single-result)))

;; updates

(defn execute!
  "Executes update statement."
  [query]
  (with-azql-context
    (let [{s :sql a :args} (sql* query)]
      (first
        (jdbc/do-prepared s a)))))

(defn- batch-arg?
  [v]
  (and (sequential? v) (:batch (meta v))))

(defn execute-return-keys!
  "Executes update statement and returns generated keys."
  [query]
  (with-azql-context
    (let [{s :sql a :args} (sql* query)]
      (check-argument
        (not-any? #(and (batch-arg? %) (not= (count %) 1)) a)
        "Can't return generated keys for batch query.")
      (jdbc/do-prepared-return-keys s (map #(if (batch-arg? %) (first %) %) a)))))

(defn- prepare-batch-arguments
  [arguments]
  (if-let [ba (first (filter batch-arg? arguments))]
    (let [cnt (count ba)
          aseqs (map (fn [a]
                       (if-not (batch-arg? a)
                         (repeat cnt a)
                         (do
                           (check-state
                             (= cnt (count a))
                             "Batch argument vectors has different lengths.")
                           a)))
                     arguments)]
      (apply map vector aseqs))
    arguments))

(defn execute-batch!
  "Executes batch statement."
  [query]
  (with-azql-context
    (let [{s :sql a :args} (sql* query)]
      (apply jdbc/do-prepared s (prepare-batch-arguments a)))))

(defn values
  "Adds records to insert statement."
  [insert records]
  (let [records (if (map? records) [records] (seq records))]
    (assoc insert
           :records (into (:records insert) records))))

(defn delete*
  "Creates new delete statement."
  ([] (->Delete nil nil nil))
  ([table] (from (delete*) table))
  ([alias table] (from (delete*) alias table)))

(defn insert*
  "Creates new insert statement."
  ([table] (->Insert (unwrap-single-table table) nil []))
  ([table records] (values (insert* table) records)))

(defn update*
  "Creates new update statement."
  ([table]
    (let [t (unwrap-single-table table)]
      (update* t t)))
  ([alias table] (->Update [alias table] nil nil)))

(defn setf*
  "Adds field to update statement."
  [query fname value]
  (assoc query :fields (assoc (:fields query) fname value)))

(defmacro setf
  "Adds field to update statement."
  [query fname value]
  `(setf* ~query ~fname ~(prepare-macro-expression value &env)))

(defmacro delete!
  "Deletes records from a table."
  [& body]
  `(execute!
     ~(emit-threaded-expression `delete* body)))

(defn execute-insert!
  "Executes insert query.
   If a single record is inserted, returns map of the generated keys."
  [{r :records :as query}]
  (if (= 1 (count r))
    (execute-return-keys! query)
    (execute-batch! query)))

(defmacro insert!
  "Inserts new record into the table.
   If a single record is inserted, returns map of the generated keys."
  [& body]
  `(execute-insert!
     ~(emit-threaded-expression `insert* body)))

(defmacro update!
  "Executes update statement."
  [& body]
  `(execute!
     ~(emit-threaded-expression `update* body)))

(defn escape-like
  "Escapes 'LIKE' pattern. Replaces all '%' with '\\%' and '_' with '\\_'."
  [^String pattern]
  (azql.expression/escape-like-pattern pattern))

; query-forms
(register-subquery-var #'select)
(register-subquery-var #'table)
(register-subquery-var #'combine)
(register-subquery-var #'union)
(register-subquery-var #'intersect)
(register-subquery-var #'except)
