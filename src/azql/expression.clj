(ns azql.expression
  (:use [azql util dialect emit]))

(defn expression-synonym
  "Replace synonym with proper function/operator name.
   This function know nothing about dialects."
  [s]
  (get '{<> not=, == =, || str} s s))

(defndialect render-true [] (raw "(0=0)"))
(defndialect render-false [] (raw "(0=1)"))

(defn- attach-field
  "Add fieldlist to query."
  [f q]
  (check-argument (nil? (:fields q)) "Relation already has fields.")
  (assoc q :fields {(as-alias-safe f) f}))

(def ^:private ^:const like-pattern-escaping-char "\\")

(defndialect like-pattern-escaping-sql
  []
  (raw (str \' like-pattern-escaping-char \')))

(defndialect escape-like-pattern
  "Escapes all '%' and '_' characters with '\\'."
  [^String a]
  (.. a
    (replace
      like-pattern-escaping-char
      (str like-pattern-escaping-char like-pattern-escaping-char))
    (replace "%" (str like-pattern-escaping-char "%"))
    (replace "_" (str like-pattern-escaping-char "_"))))

(defn render-function
  "Renders function (with or without arguments)."
  ([fname]
    (compose-sql
      (raw (name fname))
      NOSP
      (raw "()")))
  ([fname & args]
    (compose-sql
      (raw (name fname))
      NOSP
      (parentheses (comma-list args)))))

(defn- canonize-operator-symbol
  [s]
  (when-not (symbol? s)
    (illegal-argument "Illegal function name '" s "', extected symbol"))
  (expression-synonym s))

(def ^:private subquery-symbols #{})

(defn register-subquery-symbol
  [s]
  "Adds symbol to `subquery-symbols`."
  (assert (not (namespace s)))
  (alter-var-root #'subquery-symbols conj (symbol s)))

(defn subquery-form?
  "Checks is form is 'subquery'."
  [form]
  (when (seq? form)
    (let [f (first form)]
      (or
        (namespace f)
        (contains? subquery-symbols (first form))))))

(declare conj-expression)

(defn- map-style-expression?
  [m]
  (instance? clojure.lang.APersistentMap m))

(defn- normalize-map-style-expression
  [m]
  (case (count m)
    0 (render-true)
    1 (list* '= (first m))
    (list* 'and (map #(list* '= %) m))))

(defn prepare-macro-expression
  "Walks tree and replaces synonyms.
   Skips all symbols from `subquery-symbols` set.
   Ex: (+ 1 (/ :x :y)) => ['+ 1 ['divide :x :y]]"
  [e]
  (if (and
        (seq? e)
        (not (subquery-form? e)))
    (let [f (canonize-operator-symbol (first e))]
      (when-not f
        (illegal-argument "Invalid expression '" e "', unknown operator."))
      `(list
         (quote ~f)
         ~@(map prepare-macro-expression (rest e))))
    (if (map-style-expression? e)
      (prepare-macro-expression
        (normalize-map-style-expression e))
      e)))

(defn conj-expression
  "Concatenates two logical expressions with 'AND'."
  [expr e]
  (cond
    (not (seq expr)) e
    (= 'and (first expr)) (conj (vec expr) e)
    :else (vector 'and expr e)))

(def ^:private operator-rendering-fns {})

(defn- create-operator-multi
  [s]
  (clojure.lang.MultiFn.
    (str "operator-" (name s))
    current-dialect
    :default
    #'dialects-hierarchy))

(defn register-operator
  "Register new operator."
  ([op dialect f]
    (alter-var-root
      #'operator-rendering-fns
      (fn [mp]
        (let [^clojure.lang.MultiFn m
              (or
                (get mp op)
                (create-operator-multi op))]
          (.addMethod m dialect f)
          (assoc mp op m))))
    (get operator-rendering-fns op))
  ([op f]
    (register-operator op default-dialect f)))

(defn render-operator
  "Renders one function. First argument is a function symbol. Rest is args."
  [s & r]
  (if-let [f (operator-rendering-fns s)]
    (apply f r)
    (illegal-argument "Unknown operator/function '" s "'")))

(defn render-expression
  "Convert expression tree (recursively) to sql'like object."
  [etree]
  (if (and (sequential? etree) (symbol? (first etree)))
    (let [[f & r] etree
          rs (map render-expression r)]
      (apply render-operator f rs))
    etree))

(defmacro defoperator
  "Defines new operator.
   Each operator has own multimethod to dispatch by dialects."
  [s & args-and-body]
  (let [fn-name (symbol (str "operator-" (name s)))
        [d & r] (if (keyword? (first args-and-body))
                  args-and-body
                  (cons default-dialect args-and-body))]
    `(register-operator (quote ~s) ~d (fn ~fn-name ~@r))))

(defn- emit-deffunction
  [dialect [fname fsql]]
  `(defoperator ~fname ~dialect [& a#]
     (apply render-function (quote ~fsql) a#)))

(defn- as-function-name-and-sql-name
  [v]
  (cond
    (symbol? v) [[v v]]
    (sequential? v) (map vector v v)
    (map? v) (seq v)
    :else (illegal-argument "Invalid function name '" v "'.")))

(defmacro deffunctions
  "Defines functions for dialect.
   Functions can be specified by sequence of symbols
   or as map {canon-name -> dialect-name}."
  [dialect & body]
  (let [[dialect body]
        (if (keyword? dialect)
          [dialect body]
          [default-dialect (cons dialect body)])
        fs (mapcat as-function-name-and-sql-name body)]
    (list*
      `do
      (map (partial emit-deffunction dialect) fs))))

(defoperator raw
  [s]
  (raw s))

(defoperator and
  [x & r]
  (parentheses* (interpose AND (cons x r))))

(defoperator or
  [x & r]
  (parentheses* (interpose OR (cons x r))))

(defoperator =
  [a b]
  (parentheses
    (cond
      (nil? a) (compose-sql b IS_NULL)
      (nil? b) (compose-sql a IS_NULL)
      :else (compose-sql a EQUALS b))))

(defoperator not=
  [a b]
  (parentheses
    (cond
      (nil? a) (compose-sql b IS_NOT_NULL)
      (nil? b) (compose-sql a IS_NOT_NULL)
      :else (compose-sql a NOT_EQUALS b))))

(defoperator +
  ([x] (parentheses UPLUS x))
  ([x & r] (parentheses* (interpose PLUS (cons x r)))))

(defoperator -
  ([x] (parentheses UMINUS x))
  ([a & r] (parentheses* (interpose MINUS (cons a r)))))

(defoperator *
  [x & r] (parentheses* (interpose MULTIPLY (cons x r))))

(defoperator /
  [x y]
  (parentheses x DIVIDE y))

(defoperator not
  [x]
  (parentheses NOT x))

(defoperator <
  [a b]
  (parentheses a LESS b))

(defoperator >
  [a b]
  (parentheses a GREATER b))

(defoperator <=
  [a b]
  (parentheses a LESS_EQUAL b))

(defoperator >=
  [a b]
  (parentheses a GREATER_EQUAL b))

(defoperator nil?
  [x]
  (parentheses x IS_NULL))

(defoperator not-nil?
  [x]
  (parentheses x IS_NOT_NULL))

(defoperator not-in?
  [a b]
  (cond
    (empty? b) (render-true)
    (extends? SqlLike (class b)) (parentheses a NOT_IN (parentheses b))
    :else (parentheses a NOT_IN (parentheses (comma-list b)))))

(defoperator in?
  [a b]
  (cond
    (empty? b) (render-false)
    (extends? SqlLike (class b)) (parentheses a IN (parentheses b))
    :else (parentheses a IN (parentheses (comma-list b)))))

(defoperator count
  ([r] (compose-sql COUNT NOSP (parentheses r)))
  ([d r]
    (case d
      :distinct (compose-sql COUNT (parentheses DISTINCT r))
      nil [COUNT (parentheses r)]
      (illegal-argument "Unknown modifier " d))))

(defoperator max
  [x]
  (compose-sql MAX NOSP (parentheses x)))

(defoperator min
  [x]
  (compose-sql MIN NOSP (parentheses x)))

(defoperator avg
  [x]
  (compose-sql AVG NOSP (parentheses x)))

(defoperator sum
  [x]
  (compose-sql SUM NOSP (parentheses x)))

(defoperator exists?
  [q]
  (parentheses EXISTS (parentheses q)))

(defoperator not-exists?
  [q]
  (parentheses NOT_EXISTS (parentheses q)))

(defoperator some
  ([q] (compose-sql SOME (parentheses q)))
  ([f q] (compose-sql SOME (parentheses (attach-field f q)))))

(defoperator any
  ([q] (compose-sql ANY (parentheses q)))
  ([f q] (compose-sql ANY (parentheses (attach-field f q)))))

(defoperator all
  ([q] (compose-sql ALL (parentheses q)))
  ([f q] (compose-sql ALL (parentheses (attach-field f q)))))

(defoperator like?
  [a b]
  (parentheses a LIKE b ESCAPE (like-pattern-escaping-sql)))

(defoperator starts?
  [a b]
  (check-argument (string? b) "Pattern should be string.")
  (parentheses
    a LIKE (str (escape-like-pattern b) "%")
    ESCAPE (like-pattern-escaping-sql)))

(defoperator str
  [x & r] (parentheses* (interpose STR_CONCAT (cons x r))))

(defn- render-case-ext
  [v cnds]
  (compose-sql
    CASE v
    (compose-sql*
      (map
        (fn [[a b]] (compose-sql WHEN a THEN b))
        (partition 2 cnds)))
    (if (odd? (count cnds))
      (compose-sql ELSE (last cnds))
      NONE)
    END))

(defoperator case
  [v & cnds]
  (render-case-ext v cnds))

(defoperator cond
  [& cnds]
  (render-case-ext NONE cnds))
