(ns azql.render
  (:use [azql util emit expression]))

(defn- as-table-or-subquery
  "Converts value to table name or subquery.
   Surrounds subquery into parenthesis."
  [v]
  (cond
   (keyword? v) v
   (string? v) (keyword v)
   :else (parenthesis v)))

(defn- render-table
  [[alias nm]]
  (let [t (as-table-or-subquery nm)]
    (if (= alias t) t [t alias])))

(defn render-field
  [[alias nm]]
  (if (= alias nm) nm [(render-expression nm) alias]))

(defn render-fields
  [{:keys [fields tables]}]
  (if (or (nil? fields) (= fields :*))
    ASTERISK
    (interpose COMMA (map render-field fields))))

(defn render-join-type
  [jt]
  (get
   {:left LEFT_OUTER_JOIN, :right RIGHT_OUTER_JOIN,
    :full FULL_OUTER_JOIN, :inner INNER_JOIN, :cross CROSS_JOIN}
   jt jt))

(defn render-from
  [{:keys [tables joins]}]
  [FROM
   (let [[a jn] (first joins)
         t (tables a)]
     (when-not (contains? #{nil :cross} jn)
       (illegal-state "First join should be CROSS JOIN"))
     (render-table [a t]))
   (for [[a jn c] (rest joins) :let [t (tables a)]]
     (if (nil? jn)
       [COMMA (render-table [a t])]
       [(render-join-type jn)
        (render-table [a t])
        (if c [ON (render-expression c)] NONE)]))])

(defn render-where
  [{where :where}]
  (if where
    [WHERE (render-expression where)]
    NONE))

(defn render-order
  [{order :order}]
  (let [f (fn [[c d]]
            [(render-expression c)
             (get {nil NONE :asc ASC :desc DESC} d d)])]
    (if order
      [ORDER_BY (interpose COMMA (map f order))]
      NONE)))

(defn render-select
  [{:keys [fields tables joins where order] :as relation}]
  [SELECT
   (render-fields relation)
   (render-from relation)
   (render-where relation)
   (render-order relation)])
