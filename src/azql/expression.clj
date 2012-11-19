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

(defn- par
  ([a] (parentheses a))
  ([a & r] (parentheses (cons a r))))

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
    [(raw (name fname))
     NOSP
     (raw "()")])
  ([fname & args]
    [(raw (name fname))
     NOSP
     (par (comma-list args))]))

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
      (and
        (not (namespace f))
        (contains? subquery-symbols (first form))))))

(defn prepare-macro-expression
  "Walks tree and replaces synonyms.
   Skips all symbols from `subquery-symbols` set.
   Ex: (+ 1 (/ :x :y)) => ['+ 1 ['divide :x :y]]"
  [e]
  (if (and
        (list? e)
        (not (subquery-form? e)))
    (let [f (canonize-operator-symbol (first e))]
      (when-not f
        (illegal-argument "Invalid expression '" e "', unknown operator."))
      `(list
         (quote ~f)
         ~@(map prepare-macro-expression (rest e))))
    e))

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
          (assoc mp op m)))))
  ([op f]
    (register-operator op :default f)))

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
  (par (interpose AND (cons x r))))

(defoperator or
  [x & r]
  (par (interpose OR (cons x r))))

(defoperator =
  [a b]
  (par
   (cond
    (nil? a) [b IS_NULL]
    (nil? b) [a IS_NULL]
    :else [a EQUALS b])))

(defoperator not=
  [a b]
  (par
   (cond
    (nil? a) [b IS_NOT_NULL]
    (nil? b) [a IS_NOT_NULL]
    :else [a NOT_EQUALS b])))

(defoperator +
  ([x] (par [UPLUS x]))
  ([x & r] (par (interpose PLUS (cons x r)))))

(defoperator -
  ([x] (par [UMINUS x]))
  ([a & r] (par (interpose MINUS (cons a r)))))

(defoperator *
  [x & r] (par (interpose MULTIPLY (cons x r))))

(defoperator /
  [x y]
  (par [x DIVIDE y]))

(defoperator not
  [x]
  (par NOT x))

(defoperator <
  [a b]
  (par a LESS b))

(defoperator >
  [a b]
  (par a GREATER b))

(defoperator <=
  [a b]
  (par a LESS_EQUAL b))

(defoperator >=
  [a b]
  (par a GREATER_EQUAL b))

(defoperator nil?
  [x]
  (par x IS_NULL))

(defoperator not-nil?
  [x]
  (par x IS_NOT_NULL))

(defoperator not-in?
  [a b]
  (cond
   (empty? b) (render-true)
   (extends? SqlLike (class b)) (par a NOT_IN (parentheses b))
   :else (par a NOT_IN (parentheses (comma-list b)))))

(defoperator in?
  [a b]
  (cond
   (empty? b) (render-false)
   (extends? SqlLike (class b)) (par a IN (parentheses b))
   :else (par a IN (parentheses (comma-list b)))))

(defoperator count
  ([r] [COUNT NOSP (parentheses r)])
  ([d r]
     (case d
       :distinct [COUNT (par DISTINCT r)]
       nil [COUNT (par r)]
       (illegal-argument "Unknown modifier " d))))

(defoperator max
  [x]
  [MAX NOSP (par x)])

(defoperator min
  [x]
  [MIN NOSP (par x)])

(defoperator avg
  [x]
  [AVG NOSP (par x)])

(defoperator sum
  [x]
  [SUM NOSP (par x)])

(defoperator exists?
  [q]
  (par EXISTS (par q)))

(defoperator not-exists?
  [q]
  (par NOT_EXISTS (par q)))

(defoperator some
  ([q] [SOME (par q)])
  ([f q] [SOME (par (attach-field f q))]))

(defoperator any
  ([q] [ANY (par q)])
  ([f q] [ANY (par (attach-field f q))]))

(defoperator all
  ([q] [ALL (par q)])
  ([f q] [ALL (par (attach-field f q))]))

(defoperator like?
  [a b]
  (par a LIKE b ESCAPE (like-pattern-escaping-sql)))

(defoperator starts?
  [a b]
  (check-argument (string? b) "Pattern should be string.")
  (par a LIKE (str (escape-like-pattern b) "%")
   ESCAPE (like-pattern-escaping-sql)))

(defoperator str
  [x & r] (par (interpose STR_CONCAT (cons x r))))
