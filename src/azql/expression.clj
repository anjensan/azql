(ns azql.expression
  (:use [azql util emit]))

(def expression-synonym
  {'not= '<>, '== '=})

(def subquery-operators
  '#{some any all exists? not-exists? in? not-in? raw})

(def const-true (raw "(0=0)"))
(def const-false (raw "(0=1)"))

(defn- attach-field
  "Add fieldlist to query"
  [f q]
  (check-argument (nil? (:fields q)) "Relation already has fields")
  (assoc q :fields {(as-alias f) f}))

(defn- par
  ([a] (parenthesis a))
  ([a & r] (parenthesis (cons a r))))

(def ^String like-pattern-escaping-char "\\")

(def like-pattern-escaping-sql
  (raw (str \' like-pattern-escaping-char \')))

(defn escape-like-pattern
  "Escapes all '%' and '_' characters with '\\'."
  [^String a]
  (.. a
      (replace like-pattern-escaping-char
               (str like-pattern-escaping-char like-pattern-escaping-char))
      (replace "%" (str like-pattern-escaping-char "%"))
      (replace "_" (str like-pattern-escaping-char "_"))))

(def default-expression-rendering-fns
  {'raw (fn [s] (raw s))
   'and (fn [x & r] (par (interpose AND (cons x r))))
   'or  (fn [x & r] (par (interpose OR (cons x r))))
   '= (fn [a b]
        (par
         (cond
          (nil? a) [b IS_NULL]
          (nil? b) [a IS_NULL]
          :else [a EQUALS b])))
   '<> (fn [a b]
         (par
          (cond
           (nil? a) [b IS_NOT_NULL]
           (nil? b) [a IS_NOT_NULL]
           :else [a NOT_EQUALS b])))
   '+ (fn
        ([x] (par [UPLUS x]))
        ([x & r] (par (interpose PLUS (cons x r)))))
   '- (fn
        ([x] (par [UMINUS x]))
        ([a & r] (par (interpose MINUS (cons a r)))))
   '* (fn
        [x & r] (par (interpose MULTIPLY (cons x r))))
   '/ (fn
        [x y] (par [x DIVIDE y]))
   'not (fn [x](par NOT x))
   '< (fn [a b] (par a LESS b))
   '> (fn [a b] (par a GREATER b))
   '<= (fn [a b] (par a LESS_EQUAL b))
   '>= (fn [a b] (par a GREATER_EQUAL b))
   'nil? (fn [x] (par x IS_NULL))
   'not-nil? (fn [x] (par x IS_NOT_NULL))
   'not-in? (fn [a b]
              (cond
               (empty? b) const-true
               (map? b) (par a NOT_IN (parenthesis b))
               :else (par a NOT_IN (parenthesis (comma-list b)))))
   'in? (fn [a b]
          (cond
           (empty? b) const-false
           (map? b) (par a IN (parenthesis b))
           :else (par a IN (parenthesis (comma-list b)))))
   'count (fn
            ([r] [COUNT (parenthesis r)])
            ([d r]
               (case d
                :distinct [COUNT (par DISTINCT r)]
                nil [COUNT (par r)]
                (illegal-argument "Unknown modifier " d))))
   'max (fn [x] [MAX (par x)])
   'min (fn [x] [MIN (par x)])
   'avg (fn [x] [AVG (par x)])
   'sum (fn [x] [SUM (par x)])
   'exists? (fn [q] (par EXISTS (par q)))
   'not-exists? (fn [q] (par NOT_EXISTS (par q)))
   'some (fn
            ([q] [SOME (par q)])
            ([f q] [SOME (par (attach-field f q))]))
   'any (fn
            ([q] [ANY (par q)])
            ([f q] [ANY (par (attach-field f q))]))
   'all (fn
            ([q] [ALL (par q)])
            ([f q] [ALL (par (attach-field f q))]))
   'like? (fn [a b]
            [a LIKE b ESCAPE like-pattern-escaping-sql])
   'begins? (fn [a b]
                  (check-argument (string? b) "Pattern should be string.")
                  [a LIKE (str (escape-like-pattern b) "%")
                   ESCAPE like-pattern-escaping-sql])
   })

(defn render-generic-operator
  "Render generic infix operator"
  ([f x] (parenthesis [(raw (name f)) x]))
  ([f x & r]
     (parenthesis
      (interpose (raw (name f)) (cons x r)))))

(defn render-generic-function
  "Render generic function"
 ([f] (raw (str (name f) "()")))
 ([f & r]
    [(raw (str (name f) "("))
     (comma-list r)
     (raw ")")]))

(defn operator?
  [f]
  (let [fch (char (get (name f) 0))]
    (not (Character/isLetterOrDigit fch))))

(defn- canonize-operator-symbol
  [s]
  (when-not (symbol? s)
    (illegal-argument "Illegal function name '" s "', extected symbol"))
  (expression-synonym s s))

(defn render-operator-or-function
  "Render operator/function, recognize synonyms."
  [f r]
  (let [s (expression-synonym f f)]
    (if-let [c (default-expression-rendering-fns s)]
      (apply c r)
      (if (operator? s)
        (apply render-generic-operator f r)
        (apply render-generic-function f r)))))

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
  [expr e]
  (cond
   (not (seq expr)) e
   (= 'and (first expr)) (conj (vec expr) e)
   :else (list 'and expr e)))

(defn render-expression
  "Convert expression tree (recursively) to sql'like object."
  [etree]
  (if (and (sequential? etree) (symbol? (first etree)))
    (let [[f & r] etree
          rs (map render-expression r)]
      (render-operator-or-function f rs))
    etree))
