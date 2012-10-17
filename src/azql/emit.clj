(ns azql.emit
  (:use [azql util dialect])
  (:require [clojure.string :as s])
  (:use clojure.template)
  (:require [clojure.java.jdbc :as jdbc]))

(def ^:dynamic ^:private dialect-naming-strategy-installed false)

(defndialect entity-naming-strategy
  "Returns entity naming strategy for clojure/jdbc."
  []
  (fn [x] (str \" x \")))

(defmacro with-dialect-namind-strategy
  "Registers register AZQL naming strategy in `clojure/jdbc.`"
  [& body]
  `(with-dialect-namind-strategy* (fn [] ~@body)))

(defn with-dialect-namind-strategy*
  "Registers register AZQL naming strategy in `clojure/jdbc.`"
  [f]
  (if (thread-bound? #'jdbc/*as-str*)
    (f)
    (with-bindings* {#'jdbc/*as-str* (entity-naming-strategy)} f)))

(defprotocol SqlLike
  (as-sql [this] "Converts object to 'Sql'."))

(defrecord Sql [sql args]
  SqlLike
  (as-sql [this] this))

(defn sql?
  "Checks if expression is rendered (raw) SQL."
  [s]
  (instance? Sql s))

(defn raw
  "Constructs new raw SQL (without parameters)."
  ([text] (Sql. (str text) nil)))

(defn arg
  "Constructs new parameter."
  [value]
  (Sql. "?" [value]))

(defn batch-arg
  "Construct new batch parameter."
  [values]
  (Sql. "?" [(with-meta values {:batch true})]))

(defn parse-qname
  "Splits qualified name and return first part. Ex :a.val => [:a :val]."
  [qname]
  (let [n (name qname)
        rp (s/split n #"\.")]
    (mapv keyword rp)))

(defn qualifier
  "Returns first part of qualified name. Ex: :a.val => :a, :x => nil."
  [qname]
  (when qname
    (let [n (parse-qname qname)]
      (when (> (count n) 1)
        (first n)))))

(defn quote-name
  "Quotes name. Uses current jdbc naming strategy."
  [s]
  (let [s (str s)]
    (if (= s "*") s (#'jdbc/*as-str* s))))

(defn emit-qname
  "Parses and escapes qualified name.
   Uses current jdbc naming strategy.
   Ex :a.val => \"a\".\"val\"."
  [qname]
  (jdbc/as-str quote-name qname))

(defn qname
  "Constructs qualified name."
  ([qn] (Sql. (emit-qname qn) nil))
  ([qn alias] (Sql. (emit-qname [qn alias]) nil)))

(def ^{:doc "Empty token. Preserve space."} NOSP (Sql. "" nil))
(def ^{:doc "Empty token."} NONE (Sql. "" nil))

(defn- special?
  [t]
  (or (identical? t NOSP) (identical? t NONE)))

(defn- join-sql-strings
  [sqls]
  (let [sb (StringBuilder.)]
    (loop [prev-nosp true, sq sqls]
      (when-let [curr (first sq)]
        (let [nosp (identical? NOSP curr)
              spec (special? curr)]
          (when-not (or spec prev-nosp)
            (.append sb \space))
          (.append sb (:sql curr))
          (recur (and spec (or prev-nosp nosp)) (rest sq)))))
    (.toString sb)))

(extend-protocol SqlLike
  clojure.lang.Sequential
  (as-sql [this]
    (if (:batch (meta this))
      (batch-arg this)
      (let [s (map as-sql (eager-filtered-flatten this #(not (:batch (meta %)))))]
        (Sql. (join-sql-strings s)
              (seq (mapcat :args s))))))
  clojure.lang.Keyword
  (as-sql [this] (qname this))
  clojure.lang.Symbol
  (as-sql [this] (raw (name this)))
  Object
  (as-sql [this] (arg this))
  nil
  (as-sql [this] (arg nil)))

(defn sql*
  "Converts object to Sql.
   For internal usag, prefer azql.core/sql.
   Warning: this functions doesn't escape keywords."
  ([] NONE)
  ([v] (if (sql? v) v (as-sql v)))
  ([v & r] (as-sql (cons v r))))

(do-template
 [kname] (def kname (raw (str (s/replace (name 'kname) #"_" " "))))

 SELECT, FROM, WHERE, JOIN, IN, NOT_IN, ON, AND, OR, NOT, NULL, AS,
 IS_NULL, IS_NOT_NULL, ORDER_BY, GROUP_BY, HAVING, DESC, ASC, SET,
 COUNT, MIN, MAX, AVG, SUM, INSERT, DELETE, VALUES, INTO, UPDATE,
 LEFT_OUTER_JOIN, RIGHT_OUTER_JOIN, FULL_OUTER_JOIN,
 CROSS_JOIN, INNER_JOIN, DISTINCT, ALL,LIMIT, OFFSET,
 EXISTS, NOT_EXISTS, ALL, ANY, SOME,
 LIKE, ESCAPE)

(do-template
 [kname value] (def kname (raw value))

 COMMA ",",ASTERISK "*", QMARK "?",
 LEFT_PAREN "(", RIGHT_PAREN ")",
 EQUALS "=", NOT_EQUALS "<>", LESS "<", GREATER ">",
 LESS_EQUAL "<=", GREATER_EQUAL ">=", UPLUS "+",
 PLUS "+", MINUS "-", UMINUS "-", DIVIDE "/", MULTIPLY "*")

(defn parenthesis
  "Surrounds expression with parenthesis."
  [e]
  ^{::orig e} [LEFT_PAREN NOSP e NOSP RIGHT_PAREN])

(defn remove-parenthesis
  "Removes parenthesis."
  [e]
  (or (::orig (meta e)) e))

(defn comma-list
  "Returns values separated by comma."
  [values]
  (interpose [NOSP COMMA] (map remove-parenthesis values)))

(defn as-alias-safe
  "Interprets value as column/table alias."
  [n]
  (check-argument (keyword-or-string? n)
                  "Invalid alias, extected keyword or string.")
  (keyword n))

(let [sa (atom 0)]
  (defn generate-surrogate-alias
    "Generates surrogate alias."
    []
    (keyword (str "__" (swap! sa inc)))))

(defn surrogate-alias?
  "Checks if alias is surrogate."
  [n]
  (let [n (name n)]
    (re-matches #"__\d+" n)))

(defn as-alias
  "Converts value into alias if possible.
   Returns surrogate alias otherwise"
  [n]
  (cond
    (keyword? n) n
    (string? n) (keyword n)
    :else (generate-surrogate-alias)))
