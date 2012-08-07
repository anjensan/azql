(ns azql.expression
  (:use [azql util emit]))

(def expression-synonym
  {:not= :<>, :== :=, (keyword "/") :div,
   :contains? :in?})

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

(def expression-render-fn
  {:and (fn [& r] (interpose AND r))
   :or  (fn [& r] (interpose OR r))
   :+ (fn [& r] (interpose PLUS r))
   := (fn [a b]
        (cond
         (nil? a) [b IS_NULL]
         (nil? b) [a IS_NULL]
         :else [a EQUALS b]))
   :<> (fn [a b]
         (cond
          (nil? a) [b IS_NOT_NULL]
          (nil? b) [a IS_NOT_NULL]
          :else [a NOT_EQUALS b]))
   :* (fn [& r] (interpose MULTIPLY r))
   :- (fn ([x] [UMINUS x]) ([a & r] (interpose MINUS (cons a r))))
   :div (fn [x y] [x DIVIDE y])
   :not (fn [x] [NOT x])
   :< (fn [a b] [a LESS b])
   :> (fn [a b] [a GREATER b])
   :<= (fn [a b] [a LESS_EQUAL b])
   :>= (fn [a b] [a GREATER_EQUAL b])
   :nil? (fn [x] [x IS_NULL])
   :not-nil? (fn [x] [x IS_NOT_NULL])
   :not-in? (fn [a b]
              (if (empty? b)
                const-true
                [a NOT_IN (parenthesis (args-list b))]))
   :in? (fn [a b]
          (if (empty? b)
            const-false
            [a IN (parenthesis (args-list b))]))
   :count (fn [r] [COUNT (parenthesis r)])
   :count-distinct (fn [r] [COUNT (parenthesis [DISTINCT r])])
   :max (fn [x] [MAX (parenthesis x)])
   :min (fn [x] [MIN (parenthesis x)])
   :avg (fn [x] [AVG (parenthesis x)])
   :sum (fn [x] [SUM (parenthesis x)])})

(defn- canon-expr-keyword
  [f]
  (let [k (keyword (name f))]
    (get expression-synonym k k)))

(defn- find-expr-render-fn
  [f]
  (expression-render-fn (get expression-synonym f f)))

(defn expression-symbol?
  [s]
  (and
   (symbol? s)
   (not
    (nil? (find-expr-render-fn (keyword (name s)))))))

(defn prepare-macro-expression
  "Walk tree and replace symbols with keywords.
   Ex: (+ 1 :x) => [:+ 1 :x]"
  [e]
  (if (and (sequential? e) (expression-symbol? (first e)))
    (vec
     (cons
      (canon-expr-keyword (first e))
      (map prepare-macro-expression (rest e))))
    e))

(defn conj-expression
  [expr e]
  (cond
   (not (seq expr)) e
   (= :and (first expr)) (conj (vec expr) e)
   :else [:and expr e]))

(defn render-expression
  "Convert expression tree to sql'like object."
  [etree]
  (if (and (sequential? etree) (keyword? (first etree)))
    (let [[f & r] etree
          ef (find-expr-render-fn f)
          rs (map render-expression r)]
      (if (not= :count f)
        (parenthesis (apply ef rs))
        (apply ef rs)))
    etree))

(defrecord SqlFunction [fname args]
  SqlLike
  (as-sql [this]
    (sql [fname (parenthesis (comma-list args))])))

(defn sqlfn
  "Construct new function, which emits call.
   Ex: ((sqlfn :x) 1 2) => x(1, 2)"
  [f]
  (when-not (re-matches #"\w+" (name f))
    (illegal-argument "Invalid function name " f))
  (let [nm (raw (name f))]
    (fn [& args]
      (->SqlFunction nm args))))
