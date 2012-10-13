(ns azql.expression
  (:use [azql util emit dialect]))

(defn-dialect expression-synonym
  [s]
  (get {'not= '<>, '== '=} s s))

(def subquery-operators
  '#{some any all exists? not-exists? in? not-in? raw})

(defn-dialect render-true [] (raw "(0=0)"))
(defn-dialect render-false [] (raw "(0=1)"))

(defn- attach-field
  "Add fieldlist to query."
  [f q]
  (check-argument (nil? (:fields q)) "Relation already has fields.")
  (assoc q :fields {(as-alias f) f}))

(defn- par
  ([a] (parenthesis a))
  ([a & r] (parenthesis (cons a r))))

(def ^String like-pattern-escaping-char "\\")

(defn-dialect like-pattern-escaping-sql
  []
  (raw (str \' like-pattern-escaping-char \')))

(defn-dialect escape-like-pattern
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

(defn-dialect render-generic-operator
  "Render generic infix operator."
  ([f x] (parenthesis [(raw (name f)) x]))
  ([f x & r]
     (parenthesis
      (interpose (raw (name f)) (cons x r)))))

(defn-dialect render-generic-function
  "Renders generic function."
  ([f] (raw (str (name f) "()")))
  ([f & r]
    [(raw (str (name f) "("))
     (comma-list r)
     (raw ")")]))

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

(declare render-fn)

(defn-dialect render-expression
  "Convert expression tree (recursively) to sql'like object."
  [etree]
  (if (and (sequential? etree) (symbol? (first etree)))
    (let [[f & r] etree
          rs (map render-expression r)]
      (apply render-fn f rs))
    etree))

;; rendering

(defmulti render-fn
  "Renders one function. First argument is a function symbol. Rest is args."
  (fn [f & _] f))

(defmethod render-fn :default
  [s & r]
  (if (operator? s)
    (apply render-generic-operator s r)
    (apply render-generic-function s r)))

(defmacro def-op
  "Defines new operator.
   Each operator has own multimethod to dispatch by dialects."
  [s & args-and-body]
  (let [fn-name (symbol (or (:fn-name (meta s)) (str "op-" (name s))))]
    `(do
       (defn-dialect ~fn-name ~@args-and-body)
       (defmethod render-fn (quote ~s) [_# & args#] (apply ~fn-name args#)))))

(def-op raw
  [s]
  (raw s))

(def-op and
  [x & r]
  (par (interpose AND (cons x r))))

(def-op or
  [x & r]
  (par (interpose OR (cons x r))))

(def-op =
  [a b]
  (par
   (cond
    (nil? a) [b IS_NULL]
    (nil? b) [a IS_NULL]
    :else [a EQUALS b])))

(def-op <>
  [a b]
  (par
   (cond
    (nil? a) [b IS_NOT_NULL]
    (nil? b) [a IS_NOT_NULL]
    :else [a NOT_EQUALS b])))

(def-op +
  ([x] (par [UPLUS x]))
  ([x & r] (par (interpose PLUS (cons x r)))))

(def-op -
  ([x] (par [UMINUS x]))
  ([a & r] (par (interpose MINUS (cons a r)))))

(def-op *
  [x & r] (par (interpose MULTIPLY (cons x r))))

; symbol "op-/" is invalid, use "op-div" instead.
(def-op ^{:fn-name "op-div"} /
  [x y]
  (par [x DIVIDE y]))

(def-op not
  [x]
  (par NOT x))

(def-op <
  [a b]
  (par a LESS b))

(def-op >
  [a b]
  (par a GREATER b))

(def-op <=
  [a b]
  (par a LESS_EQUAL b))

(def-op >=
  [a b]
  (par a GREATER_EQUAL b))

(def-op nil?
  [x]
  (par x IS_NULL))

(def-op not-nil?
  [x]
  (par x IS_NOT_NULL))

(def-op not-in?
  [a b]
  (cond
   (empty? b) (render-true)
   (map? b) (par a NOT_IN (parenthesis b))
   :else (par a NOT_IN (parenthesis (comma-list b)))))

(def-op in?
  [a b]
  (cond
   (empty? b) (render-false)
   (map? b) (par a IN (parenthesis b))
   :else (par a IN (parenthesis (comma-list b)))))

(def-op count
  ([r] [COUNT (parenthesis r)])
  ([d r]
     (case d
       :distinct [COUNT (par DISTINCT r)]
       nil [COUNT (par r)]
       (illegal-argument "Unknown modifier " d))))

(def-op max
  [x]
  [MAX (par x)])

(def-op min
  [x]
  [MIN (par x)])

(def-op avg
  [x]
  [AVG (par x)])

(def-op sum
  [x]
  [SUM (par x)])

(def-op exists?
  [q]
  (par EXISTS (par q)))

(def-op not-exists?
  [q]
  (par NOT_EXISTS (par q)))

(def-op some
  ([q] [SOME (par q)])
  ([f q] [SOME (par (attach-field f q))]))

(def-op any
  ([q] [ANY (par q)])
  ([f q] [ANY (par (attach-field f q))]))

(def-op all
  ([q] [ALL (par q)])
  ([f q] [ALL (par (attach-field f q))]))

(def-op like?
  [a b]
  [a LIKE b ESCAPE (like-pattern-escaping-sql)])

(def-op begins?
  [a b]
  (check-argument (string? b) "Pattern should be string.")
  [a LIKE (str (escape-like-pattern b) "%")
   ESCAPE (like-pattern-escaping-sql)])
