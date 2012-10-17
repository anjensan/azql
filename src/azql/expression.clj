(ns azql.expression
  (:use [azql util emit dialect]))

(defndialect expression-synonym
  [s]
  (get {'not= '<>, '== '=} s s))

(def subquery-operators
  '#{some any all exists? not-exists? in? not-in? raw})

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

(def ^String like-pattern-escaping-char "\\")

(defndialect like-pattern-escaping-sql
  []
  (raw (str \' like-pattern-escaping-char \')))

(defndialect escape-like-pattern
  "Escapes all '%' and '_' characters with '\\'."
  [^String a]
  (.. a
      (replace like-pattern-escaping-char
               (str like-pattern-escaping-char like-pattern-escaping-char))
      (replace "%" (str like-pattern-escaping-char "%"))
      (replace "_" (str like-pattern-escaping-char "_"))))

(defn- operator?
  [f]
  (let [fch (char (get (name f) 0))]
    (not (Character/isLetterOrDigit fch))))

(defndialect render-generic-operator
  "Render generic infix operator."
  ([f x] (parentheses [(raw (name f)) x]))
  ([f x & r]
     (parentheses
      (interpose (raw (name f)) (cons x r)))))

(defndialect render-generic-function
  "Renders generic function."
  ([f] [(raw (name f)) NOSP (raw "()")])
  ([f & r]
    [(raw (name f))
     NOSP LEFT_PAREN NOSP
     (comma-list r)
     NOSP RIGHT_PAREN]))

(defn- canonize-operator-symbol
  [s]
  (when-not (symbol? s)
    (illegal-argument "Illegal function name '" s "', extected symbol"))
  (expression-synonym s))

(defn prepare-macro-expression
  "Walk tree and replace synonyms.
   Ex: (+ 1 (/ :x :y)) => ['+ 1 ['divide :x :y]]"
  [e]
  (if (list? e)
    (let [f (canonize-operator-symbol (first e))]
      (when-not f
        (illegal-argument "Invalid expression '" e "', unknown operator."))
      (vec
       (cons
        `(quote ~f)
        (if (contains? subquery-operators f)
          (rest e)
          (map prepare-macro-expression (rest e))))))
    e))

(defn conj-expression
  "Concatenates two logical expressions with 'AND'."
  [expr e]
  (cond
   (not (seq expr)) e
   (= 'and (first expr)) (conj (vec expr) e)
   :else (list 'and expr e)))

(def operator-rendering-fns {})

(defn- create-operator-multi
  [s]
  (clojure.lang.MultiFn.
    (str "operator-" (name s))
    current-dialect
    :default
    #'dialects-hierarchy))

(defn add-operator
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
    (add-operator op :default f)))

(defn render-operator
  "Renders one function. First argument is a function symbol. Rest is args."
  [s & r]
  (if-let [f (operator-rendering-fns s)]
    (apply f r)
    (if (operator? s)
      (apply render-generic-operator s r)
      (apply render-generic-function s r))))

(defn render-expression
  "Convert expression tree (recursively) to sql'like object."
  [etree]
  (if (and (sequential? etree) (symbol? (first etree)))
    (let [[f & r] etree
          rs (map render-expression r)]
      (apply render-operator f rs))
    etree))

;; rendering

(defmacro defoperator
  "Defines new operator.
   Each operator has own multimethod to dispatch by dialects."
  [s & args-and-body]
  (let [fn-name (symbol (str "operator-" (name s)))
        [d & r] (if (keyword? (first args-and-body))
                args-and-body
                (cons default-dialect args-and-body))]
    `(add-operator (quote ~s) ~d (fn ~fn-name ~@r))))

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

(defoperator <>
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
   (map? b) (par a NOT_IN (parentheses b))
   :else (par a NOT_IN (parentheses (comma-list b)))))

(defoperator in?
  [a b]
  (cond
   (empty? b) (render-false)
   (map? b) (par a IN (parentheses b))
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

(defoperator begins?
  [a b]
  (check-argument (string? b) "Pattern should be string.")
  (par a LIKE (str (escape-like-pattern b) "%")
   ESCAPE (like-pattern-escaping-sql)))
