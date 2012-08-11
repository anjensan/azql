(ns azql.emit
  (:use azql.util)
  (:require [clojure.string :as s])
  (:use clojure.template))

(defprotocol SqlLike
  (as-sql [this] "Converts object to 'Sql'"))

(defrecord Sql [sql args]
  SqlLike
  (as-sql [this] this))

(defn sql?
  "Check if expression is rendered (raw) SQL"
  [s]
  (instance? Sql s))

(defn raw
  "Constructs new raw SQL (without parameters)"
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
  "Split qualified name and return first part. Ex :a.val => [:a :val]"
  [qname]
  (let [n (name qname)
        rp (s/split n #"\.")]
    (mapv keyword rp)))

(defn qualifier
  "Return first part of qualified name. Ex: :a.val => :a, :x => nil"
  [qname]
  (when qname
    (let [n (parse-qname qname)]
      (when (> (count n) 1)
        (first n)))))

(defn ^:dynamic quote-name
  "Quote name."
  [s]
  (str \" s \"))

(defn- emit-quoted-qname-part
  [p]
  (let [n (name p)]
    (if (= "*" n)
      n
      (quote-name n))))

(defn emit-qname
  "Parse qualified name and return SQL. Ex :a.val => \"a\".\"val\""
  [qname]
  (let [r (parse-qname qname)]
    (s/join \. (map emit-quoted-qname-part r))))

(defn qname
  "Constructs qualified name."
  ([qn] (Sql. (emit-qname qn) nil))
  ([qn alias] (Sql. (emit-qname [qn alias]) nil)))

(defn- insert-space?
  [^String a ^String b]
  (not
   (or
    (nil? a)
    (nil? b)
    (.isEmpty b)
    (.startsWith b ")")
    (.endsWith a "(")
    (.startsWith b ","))))

(defn- join-sql-strings
  [strings]
  (let [sb (StringBuilder.)]
    (reduce (fn [prev curr]
              (when (and (not (nil? prev)) (insert-space? prev curr))
                (.append sb \space))
              (.append sb curr)
              curr) nil strings)
    (str sb)))

(extend-protocol SqlLike
  clojure.lang.Sequential
  (as-sql [this]
    (if (:batch (meta this))
      (batch-arg this)
      (let [s (map as-sql (eager-filtered-flatten this #(not (:batch (meta %)))))]
        (Sql. (join-sql-strings (map :sql s))
              (mapcat :args s)))))
  clojure.lang.Keyword
  (as-sql [this] (qname this))
  clojure.lang.Symbol
  (as-sql [this] (raw (name this)))
  Object
  (as-sql [this] (arg this))
  nil
  (as-sql [this] (arg nil)))

(defn sql
  "Converts object to Sql."
  ([v]
     (if (sql? v)
       v
       (let [v (as-sql v)]
         (assoc v :args (vec (:args v))))))
  ([v & r] (sql (cons v r))))

(do-template
 [kname] (def kname (raw (str (s/replace (name 'kname) #"_" " "))))

 SELECT, FROM, WHERE, JOIN, IN, NOT_IN, ON, AND, OR, NOT, NULL, AS,
 IS_NULL, IS_NOT_NULL, ORDER_BY, GROUP_BY, HAVING, DESC, ASC, SET,
 COUNT, MIN, MAX, AVG, SUM, INSERT, DELETE, VALUES, INTO, UPDATE,
 LEFT_OUTER_JOIN, RIGHT_OUTER_JOIN, FULL_OUTER_JOIN,
 CROSS_JOIN, INNER_JOIN, DISTINCT, ALL,LIMIT, OFFSET)

(do-template
 [kname value] (def kname (raw value))

 NONE "", COMMA ",",ASTERISK "*", QMARK "?", LP "(", RP ")",
 EQUALS "=", NOT_EQUALS "<>", LESS "<", GREATER ">",
 LESS_EQUAL "<=", GREATER_EQUAL ">=",
 PLUS "+", MINUS "-", UMINUS "-", DIVIDE "/", MULTIPLY "*")

(defn parenthesis
  "Surrounds expression with parenthesis."
  [e]
  [LP e RP])

(defn comma-list
  "Returns values separated by comma."
  [values]
  (interpose COMMA values))