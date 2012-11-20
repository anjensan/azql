(ns azql.emit
  (:use [azql util dialect])
  (:use clojure.template)
  (:require [clojure.string :as s])
  (:require [clojure.java.jdbc :as jdbc])
  (:import java.util.regex.Matcher))

(def ^:dynamic ^:private dialect-naming-strategy-installed false)

(defndialect entity-naming-strategy
  "Returns entity naming strategy for clojure/jdbc."
  []
  (fn [x] (str \" x \")))

(defmacro with-dialect-naming-strategy
  "Registers register AZQL naming strategy in `clojure/jdbc.`"
  [& body]
  `(with-dialect-naming-strategy* (fn [] ~@body)))

(defn with-dialect-naming-strategy*
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

(def ^{:doc "Empty token. Preserve space."} NOSP (raw ""))
(def ^{:doc "Empty token."} NONE (raw ""))

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
      (illegal-argument "Can't convert '" (vec this) "' to sql.")))
  clojure.lang.Keyword
  (as-sql [this] (qname this))
  clojure.lang.Symbol
  (as-sql [this] (raw (name this)))
  Object
  (as-sql [this] (arg this))
  nil
  (as-sql [this] #=(arg nil)))

(declare compose-sql)
(declare compose-sql*)
(declare join-sqls)

(deftype ComposedSql [sqls meta]
  SqlLike
  (as-sql [this] (join-sqls (.sqls this)))
  clojure.lang.Sequential
  clojure.lang.Seqable
  (seq [this] (seq (.sqls this)))
  clojure.lang.IObj
  (withMeta [this m] (ComposedSql. sqls m))
  (meta [this] (.-meta this)))

(def ^:dynamic composed-sql-print-ident 0)

(defmethod print-method ComposedSql [^ComposedSql cs ^Appendable w]
  (let [sqls (.sqls cs)]
    (.append w "#<[")
    (doseq [s sqls]
      (.append w \space)
      (print-method s w)
      (.append w \space))
    (.append w "]>")))

(defn- join-sqls
  ([s]
    (let [sr (eager-filtered-flatten (ComposedSql. s nil) #(instance? ComposedSql %))
          s (map as-sql sr)]
      (Sql. (join-sql-strings s)
            (seq (mapcat :args s))))))

(defn compose-sql
  "Composes SQL-like objects into one."
  ([] NONE)
  ([a]
    (if (instance? ComposedSql a)
      a
      (ComposedSql. [a] nil)))
  ([a & r]
    (ComposedSql. (cons a r) nil)))

(defn compose-sql*
  "Composes SQL-like objects into one.
   Last arg treated as a sequence (similar to `list*`)."
  ([] NONE)
  ([& a]
    (let [s (apply list* a)]
      (if (empty? s)
        NONE
        (ComposedSql. s nil)))))

(defn sql*
  "Converts object to Sql.
   For internal usag, prefer azql.core/sql.
   Warning: this functions doesn't escape keywords."
  ([] NONE)
  ([v & r] (as-sql (compose-sql* v r))))

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
 PLUS "+", MINUS "-", UMINUS "-", DIVIDE "/", MULTIPLY "*",
 STR_CONCAT "||")


; misc

(defn parentheses
  "Surrounds token with parentheses."
  [e]
  (with-meta
    (compose-sql LEFT_PAREN NOSP e NOSP RIGHT_PAREN)
    {::without-parentheses e}))

(defn remove-parentheses
  "Removes parenthesis."
  [e]
  (or (::without-parentheses (meta e)) e))

(defn comma-list
  "Returns values separated by comma."
  [values]
  (compose-sql*
    (interpose (compose-sql NOSP COMMA) (map remove-parentheses values))))

(defn as-alias-safe
  "Interprets value as column/table alias."
  [n]
  (check-argument (keyword-or-string? n)
                  "Invalid alias, extected keyword or string.")
  (keyword n))

(def surrogate-alias-counter (atom 0))

(defn generate-surrogate-alias
  "Generates surrogate alias."
  []
  (let [k (swap! surrogate-alias-counter  #(-> % inc (mod 1000000)))]
    (keyword (format "__%06d" k))))

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

(defn format-interpolated-sql-arg
  [a]
  (cond
    (string? a)
    (str \' (s/replace a #"'" "''") \'),
    (sequential? a)
    (str "[" (s/join " " (map format-interpolated-sql-arg a)) "]"),
    (nil? a)
    "NULL",
    :else
    (str a)))

(defn ^String interpolate-sql
  "Replaces placeholders with actual values.
   For debug purposes only!"
  [q]
  (let [{ss :sql as :args} (as-sql q)]
    (reduce
      (fn [s a]
        (s/replace-first
          s #"\?"
          (Matcher/quoteReplacement
            (format-interpolated-sql-arg a))))
      ss as)))

(defn- parse-placeholders
  [query]
  (map
    (comp keyword second)
    (re-seq #":([\w.-]+)" query)))

(defn- replace-placeholders-with-mark
  [query]
  (s/replace query #":[\w.-]+" "?"))

(defn format-sql
  "Formats a raw sql qeury (string).
   Replaces all keyword-like placeholders with '?'.
   Args should be a map."
  [raw-sql args]
  (let [al (parse-placeholders raw-sql)
        pq (replace-placeholders-with-mark raw-sql)
        akeys (set (keys args))]
    (when-let [ma (seq (remove akeys al))]
      (illegal-argument
        (str "Unknown arguments " (vec ma) " in sql.")))
    (->Sql pq (map args al))))
