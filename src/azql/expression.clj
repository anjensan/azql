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

; TODO: dispatch on dialect

(defmulti render-fn (fn [f & _] f))

(defmethod render-fn :default
  [s & r]
  (if (operator? s)
    (apply render-generic-operator s r)
    (apply render-generic-function s r)))

(defmethod render-fn 'raw
  [_ s]
  (raw s))

(defmethod render-fn 'and
  [_ x & r]
  (par (interpose AND (cons x r))))

(defmethod render-fn 'or
  [_ x & r]
  (par (interpose OR (cons x r))))

(defmethod render-fn '=
  [_ a b]
  (par
   (cond
    (nil? a) [b IS_NULL]
    (nil? b) [a IS_NULL]
    :else [a EQUALS b])))

(defmethod render-fn '<>
  [_ a b]
  (par
   (cond
    (nil? a) [b IS_NOT_NULL]
    (nil? b) [a IS_NOT_NULL]
    :else [a NOT_EQUALS b])))

(defmethod render-fn '+
  ([_ x] (par [UPLUS x]))
  ([_ x & r] (par (interpose PLUS (cons x r)))))

(defmethod render-fn '-
  ([_ x] (par [UMINUS x]))
  ([_ a & r] (par (interpose MINUS (cons a r)))))

(defmethod render-fn '*
  [_ x & r] (par (interpose MULTIPLY (cons x r))))

(defmethod render-fn '/
  [_ x y] (par [x DIVIDE y]))

(defmethod render-fn 'not
  [_ x]
  (par NOT x))

(defmethod render-fn '<
  [_ a b]
  (par a LESS b))

(defmethod render-fn '>
  [_ a b]
  (par a GREATER b))

(defmethod render-fn '<=
  [_ a b]
  (par a LESS_EQUAL b))

(defmethod render-fn '>=
  [_ a b]
  (par a GREATER_EQUAL b))

(defmethod render-fn 'nil?
  [_ x]
  (par x IS_NULL))

(defmethod render-fn 'not-nil?
  [_ x]
  (par x IS_NOT_NULL))

(defmethod render-fn 'not-in?
  [_ a b]
  (cond
   (empty? b) (render-true)
   (map? b) (par a NOT_IN (parenthesis b))
   :else (par a NOT_IN (parenthesis (comma-list b)))))

(defmethod render-fn 'in?
  [_ a b]
  (cond
   (empty? b) (render-false)
   (map? b) (par a IN (parenthesis b))
   :else (par a IN (parenthesis (comma-list b)))))

(defmethod render-fn 'count
  ([_ r] [COUNT (parenthesis r)])
  ([_ d r]
     (case d
       :distinct [COUNT (par DISTINCT r)]
       nil [COUNT (par r)]
       (illegal-argument "Unknown modifier " d))))

(defmethod render-fn 'max
  [_ x]
  [MAX (par x)])

(defmethod render-fn 'min
  [_ x]
  [MIN (par x)])

(defmethod render-fn 'avg
  [_ x]
  [AVG (par x)])

(defmethod render-fn 'sum
  [_ x]
  [SUM (par x)])

(defmethod render-fn 'exists?
  [_ q]
  (par EXISTS (par q)))

(defmethod render-fn 'not-exists?
  [_ q]
  (par NOT_EXISTS (par q)))

(defmethod render-fn 'some
  ([_ q] [SOME (par q)])
  ([_ f q] [SOME (par (attach-field f q))]))

(defmethod render-fn 'any
  ([_ q] [ANY (par q)])
  ([_ f q] [ANY (par (attach-field f q))]))

(defmethod render-fn 'all
  ([_ q] [ALL (par q)])
  ([_ f q] [ALL (par (attach-field f q))]))

(defmethod render-fn 'like?
  [_ a b]
  [a LIKE b ESCAPE (like-pattern-escaping-sql)])

(defmethod render-fn 'begins?
  [_ a b]
  (check-argument (string? b) "Pattern should be string.")
  [a LIKE (str (escape-like-pattern b) "%")
   ESCAPE (like-pattern-escaping-sql)])
