(ns azql.core
  (:use [azql util dialect connection emit expression render])
  (:use clojure.template)
  (:require [clojure.string :as s])
  (:require [clojure.java.jdbc :as jdbc]))

(defmacro with-azql-context
  "Executes code in azql context (global connection, naming strategy ect).
   All public functions automatically call this macro. For internal use only!"
  [db & body]
  `(->> (do ~@body)
        (with-recognized-dialect)
        (with-azql-connection ~db)))

(defn sql
  "Convert string to Sql."
  ([raw-sql] (raw raw-sql))
  ([raw-sql params] (format-sql raw-sql params)))

(defrecord Select
  [tables joins fields where
   group having order
   modifier offset limit]
  Query
  SqlLike
  (as-sql [this] (as-sql (render-select this))))

(defrecord Insert [table records]
  SqlLike
  (as-sql [this] (as-sql (render-insert this))))

(defrecord Delete [table where]
  SqlLike
  (as-sql [this] (as-sql (render-delete this))))

(defrecord Update [table field-exprs where]
  SqlLike
  (as-sql [this] (as-sql (render-update this))))

(defrecord CombinedSelect
  [type queries order limit offset modifier]
  Query
  SqlLike
  (as-sql [this] (as-sql (render-combined this))))

(defrecord LazySelect [content-fn]
  Query
  SqlLike
  (as-sql [this] (content-fn)))

(defrecord RenderedSelect [content]
  Query
  SqlLike
  (as-sql [this] (as-sql content)))

(do-template
  [v]
  (defmethod print-method v [s ^Appendable w]
    (.append w (interpolate-sql s)))
  Select Insert Update Delete CombinedSelect)

(defmethod print-method LazySelect [^LazySelect s ^Appendable w]
  (.append w (interpolate-sql ((.content-fn s)))))

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
           original# (fn ~args (as-sql (select ~@body)))
           compile# (fn [] (apply original# ~sargs))]
       (defn ~name ~args
         (->LazySelect
          (fn []
            (let [dialect# (current-dialect)
                  cached-sql# (get @sqls# dialect#)
                  sql# (if cached-sql#
                         cached-sql#
                         (let [new-sql# (compile#)]
                           (swap! sqls# assoc dialect# new-sql#)
                           new-sql#))
                  args# (:args sql#)]
              (assoc sql# :args (replace ~sargs-args-map args#)))))))))

(defn- emit-raw-select
  [name args sql]
  (let [args-map (into {} (map (juxt keyword identity) args))]
    `(defn ~name ~args
       (->RenderedSelect
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
  (check-type query [Select] "Firt argument must be a Select")
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
     `(join* ~relation ~join-key ~alias ~table
             ~(prepare-macro-expression cond &env)))
   ([relation table cond]
     `(let [table# ~table]
        (join* ~relation ~join-key nil table#
               ~(prepare-macro-expression cond &env)))))
  join-inner :inner, join :inner,
  join-right :right, join-left :left, join-full :full)

(defn fields*
  "Adds field list to query."
  [query fd]
  (check-type query [Select] "Firt argument must be a Query")
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
    (check-type relation [Select CombinedSelect] "Firt argument must be a Select")
    (check-argument
      (contains? #{:asc :desc nil} dir)
      (str "Invalid sort direction " dir))
    (assoc
      relation
      :order (cons [column dir] order))))

(defmacro order
  "Adds 'order by' section to query."
  ([relation column]
     `(order* ~relation ~(prepare-macro-expression column &env)))
  ([relation column dir]
     `(order* ~relation ~(prepare-macro-expression column &env) ~dir)))

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
  (check-type relation [Select CombinedSelect] "Firt argument must be a Select")
  (check-argument (nil? cm) (str "Relation already has modifier " cm))
  (check-argument (or (sql? m) (#{:distinct :all} m))
                  "Invalid modifier, expected :distinct, :all or raw sql.")
  (assoc relation :modifier m))

(defn limit
  "Limits number of rows."
  [{ov :limit :as relation} v]
  (check-type relation [Select CombinedSelect] "Firt argument must be a Select")
  (check-state (nil? ov) (str "Relation already has limit " ov))
  (assoc relation :limit v))

(defn offset
  "Adds an offset to query."
  [{ov :offset :as relation} v]
  (check-type relation [Select CombinedSelect] "Firt argument must be a Select")
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
  (check-type combined [CombinedSelect] "Firt argument must be a CombinedSelect")
  (assoc combined :queries (conj (or (:queries combined) []) query)))

(defn combine*
  [type]
  (assoc #azql.core.CombinedSelect{} :type type))

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
  (let [{s :sql p :args} (as-sql relation)]
    (apply vector s p)))

(defmacro with-fetch
  "Executes a query & evaluates body with 'v' bound to seq of results."
  [db [v relation :as vr] & body]
  (check-argument (vector? vr))
  (check-argument (= 2 (count vr)))
  `(with-azql-context ~db
     (let [sql-params# (#'to-sql-params ~relation)
           f# (fn [~v] ~@body)]
       (jdbc/query
        *db*
        sql-params#
        :result-set-fn f#
        :row-fn identity))))

(defn fetch-all
  "Executes query and returns results as a vector."
  ([db relation]
     (with-azql-context db
       (jdbc/query
        *db*
        (to-sql-params relation)
        :result-set-fn vec))))

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
  ([db relation]
     (with-azql-context db
       (jdbc/query
        *db*
        (to-sql-params relation)
        :result-set-fn one-result))))

(defn fetch-single
  "Executes quiery and return single result value. Useful for aggregate functions."
  ([db relation]
     (with-azql-context db
       (jdbc/query
        *db*
        (to-sql-params relation)
        :result-set-fn single-result))))

;; updates

(defn execute!
  "Executes update statement."
  ([db query]
     (with-azql-context db
       (let [{s :sql a :args} (as-sql query)]
         (first
          (jdbc/db-do-prepared
           *db* false s a))))))

(defn- batch-arg?
  [v]
  (and (sequential? v) (:batch (meta v))))

(defn execute-return-keys!
  "Executes update statement and returns generated keys."
  [db query]
  (with-azql-context db
    (let [{s :sql a :args} (as-sql query)]
      (check-argument
        (not-any? #(and (batch-arg? %) (not= (count %) 1)) a)
        "Can't return generated keys for batch query.")
      (jdbc/db-do-prepared-return-keys
       *db*
       false
       s
       (map #(if (batch-arg? %) (first %) %) a)))))

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
  [db query]
  (with-azql-context db
    (let [{s :sql a :args} (as-sql query)]
      (apply jdbc/db-do-prepared *db* false s (prepare-batch-arguments a)))))

(defn values
  "Adds records to insert statement."
  [insert records]
  (let [records (if (map? records) [records] (seq records))]
    (assoc insert
           :records (into (:records insert) records))))

(defn delete*
  "Creates new delete statement."
  ([table] (->Delete (unwrap-single-table table) nil)))

(defn insert*
  "Creates new insert statement."
  ([table] (->Insert (unwrap-single-table table) []))
  ([table records] (values (insert* table) records)))

(defn update*
  "Creates new update statement."
  ([table] (->Update (unwrap-single-table table) nil nil)))

(defn setf*
  "Adds field to update statement."
  [query fname value]
  (assoc query :field-exprs (assoc (:field-exprs query) fname value)))

(defmacro setf
  "Adds field to update statement."
  [query fname value]
  `(setf* ~query ~fname ~(prepare-macro-expression value &env)))

(defmacro delete!
  "Deletes records from a table."
  [db & body]
  `(execute! ~db
     ~(emit-threaded-expression `delete* body)))

(defn execute-insert!
  "Executes insert query.
   If a single record is inserted, returns map of the generated keys."
  ([db {r :records :as query}]
     (if (= 1 (count r))
       (execute-return-keys! db query)
       (execute-batch! db query))))

(defmacro insert!
  "Inserts new record into the table.
   If a single record is inserted, returns map of the generated keys."
  [db & body]
  `(execute-insert! ~db
     ~(emit-threaded-expression `insert* body)))

(defmacro update!
  "Executes update statement."
  [db & body]
  `(execute! ~db
     ~(emit-threaded-expression `update* body)))

(defn escape-like
  "Escapes 'LIKE' pattern. Replaces all '%' with '\\%' and '_' with '\\_'."
  [^String pattern]
  (azql.expression/escape-like-pattern pattern))

; transactions

(defn with-connection*
  "Run function with open connection."
  [db body-fn]
  (check-state (not (jdbc/db-find-connection db)) "Connection already exists!")
  (with-open [^java.sql.Connection conn (jdbc/get-connection db)]
    (body-fn (jdbc/add-connection db conn))))

(defmacro with-connection
  "Executes block of code with open connection."
  [binding & body]
  `(with-connection* ~(second binding) (fn [~(first binding)] ~@body)))

(defn- parse-isolation-level
  [isolation-level]
  (int
   (get
    {:none java.sql.Connection/TRANSACTION_NONE,
     :read-committed java.sql.Connection/TRANSACTION_READ_COMMITTED,
     :read-uncommitted java.sql.Connection/TRANSACTION_READ_UNCOMMITTED,
     :repeatable-read java.sql.Connection/TRANSACTION_REPEATABLE_READ,
     :serializable java.sql.Connection/TRANSACTION_SERIALIZABLE}
    isolation-level isolation-level)))

(defn transaction*
  "Evaluates func in scope of transaction on open database connection."
  ([db-conn isolation-level body-fn]
     (io!
      (check-state (jdbc/db-find-connection db-conn)
                   "No open connection found! Use 'with-connection' macro.")
      (let [^java.sql.Connection c (jdbc/db-connection db-conn)
            old-il (.getTransactionIsolation c)]
        (check-state (.getAutoCommit c) "Connection is not in autocommit mode.")
        (when-not (nil? isolation-level)
          (.setTransactionIsolation c (parse-isolation-level isolation-level)))
        (try
          (.setAutoCommit c false)
          (let [res (body-fn)] (.commit c) res)
          (catch Throwable t
            (.rollback c))
          (finally
            (when-not (nil? isolation-level)
              (.setTransactionIsolation c old-il))
            (.setAutoCommit c true))))))
  ([db-conn body-fn]
     (transaction* db-conn nil body-fn)))

(defmacro transaction
  "Evaluates body in the context of a transaction
   on the specified database connection."
  [ilevel-or-db & body]
  (let [[il db body]
        (if (or (integer? ilevel-or-db) (keyword? ilevel-or-db))
          [ilevel-or-db (first body) (rest body)]
          [nil ilevel-or-db body])]
  `(transaction* ~db ~il (fn [] ~@body))))

; query-forms
(register-subquery-var #'select)        ;
(register-subquery-var #'table)
(register-subquery-var #'combine)
(register-subquery-var #'union)
(register-subquery-var #'intersect)
(register-subquery-var #'except)
