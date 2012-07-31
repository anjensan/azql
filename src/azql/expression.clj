(ns azql.expression
  (:use [azql emit])
  (:use clojure.walk))

;; TODO: add LIKE, ANY, SOME, EXISTS etc

(def ^:private expr-synonims
  {:not= :<>, :== :=, (keyword "/") :div})

(def ^:private expr-rendering-fn
  {:and (fn [& r] (interpose AND r))
   :or  (fn [& r] (interpose OR r))
   :+ (fn [& r] (interpose PLUS r))
   := (fn [a b] [a EQUALS b])
   :<> (fn [a b] [a NOT_EQUALS b])
   :* (fn [& r] (interpose MULTIPLY r))
   :- (fn ([x] [UMINUS x]) ([a & r] (interpose MINUS (cons a r))))
   :div (fn [x y] [x DIVIDE y])
   :not (fn [x] [NOT x])
   :< (fn [a b] [a LESS b])
   :> (fn [a b] [a GREATER b])
   :<= (fn [a b] [a LESS_EQUAL b])
   :>= (fn [a b] [a GREATER_EQUAL b])
   :nil? (fn [x] [IS_NULL x])
   :not-nil? (fn [x] [IS_NOT_NULL x])
   :in (fn [a b] [a IN b])
   :not-in (fn [a b] [a NOT_IN b])})

(defn- canon-expr-keyword
  [f]
  (let [k (keyword (name f))]
    (get expr-synonims k k)))

(defn- find-expr-rendering-fn
  [f]
  (expr-rendering-fn
   (get expr-synonims f f)))

(defn expression-symbol?
  [s]
  (and
   (symbol? s)
   (not
    (nil? (find-expr-rendering-fn (keyword (name s)))))))

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
  "Convert expressions tree to sql'like object."
  [etree]
  (if (and (sequential? etree) (keyword? (first etree)))
    (let [[f & r] etree
          ef (find-expr-rendering-fn f)
          rs (map render-expression r)]
      (parenthesis (apply ef rs)))
    etree))
