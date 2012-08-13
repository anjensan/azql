(ns azql.expression
  (:use [azql util emit]))

(def expression-synonym
  {'not= '<>, '== '=})

(defn- args-list-size
  [c]
  {:pre [(pos? c)]
   :post [(>= % c)]}
  (let [b (cond (< c 16) 4, (< c 64) 16 :else 32)]
    (-> c (dec) (+ b) (quot b) (* b))))

(defn- args-list
  [v]
  (let [s (args-list-size (count v))
        a (map arg v)]
      (comma-list (take s (cycle a)))))

(def const-true (raw "(0=0)"))
(def const-false (raw "(0=1)"))

(def default-expression-rendering-fns
  {'and (fn [x & r] (interpose AND (cons x r)))
   'or  (fn [x & r] (interpose OR (cons x r)))
   '= (fn [a b]
        (cond
         (nil? a) [b IS_NULL]
         (nil? b) [a IS_NULL]
         :else [a EQUALS b]))
   '<> (fn [a b]
         (cond
          (nil? a) [b IS_NOT_NULL]
          (nil? b) [a IS_NOT_NULL]
          :else [a NOT_EQUALS b]))
   '+ (fn ([x] [UPLUS x]) ([x & r] (interpose PLUS (cons x r))))
   '- (fn ([x] [UMINUS x]) ([a & r] (interpose MINUS (cons a r))))
   '* (fn [x & r] (interpose MULTIPLY (cons x r)))
   '/ (fn [x y] [x DIVIDE y])
   'not (fn [x] [NOT x])
   '< (fn [a b] [a LESS b])
   '> (fn [a b] [a GREATER b])
   '<= (fn [a b] [a LESS_EQUAL b])
   '>= (fn [a b] [a GREATER_EQUAL b])
   'nil? (fn [x] [x IS_NULL])
   'not-nil? (fn [x] [x IS_NOT_NULL])
   'not-in? (fn [a b]
              (if (empty? b)
                const-true
                [a NOT_IN (parenthesis (args-list b))]))
   'in? (fn [a b]
          (if (empty? b)
            const-false
            [a IN (parenthesis (args-list b))]))
   'count (fn
            ([r] [COUNT (parenthesis r)])
            ([d r]
               (case d
                :distinct [COUNT (parenthesis [DISTINCT r])]
                nil [COUNT (parenthesis r)]
                (illegal-argument "Unknown modifier " d))))
   'max (fn [x] [MAX (parenthesis x)])
   'min (fn [x] [MIN (parenthesis x)])
   'avg (fn [x] [AVG (parenthesis x)])
   'sum (fn [x] [SUM (parenthesis x)])})

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
      (parenthesis (apply c r))
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
      (vec (cons `(quote ~f) (map prepare-macro-expression (rest e)))))
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
