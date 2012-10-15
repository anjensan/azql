(ns azql.render
  (:use [azql util emit expression dialect])
  (:require [clojure.set :as cset])
  (:require [clojure.java.jdbc :as jdbc]))

(defn- as-table-or-subquery
  "Converts value to table name or subquery.
   Surrounds subquery into parenthesis."
  [v]
  (cond
   (keyword? v) v
   (string? v) (keyword v)
   :else (parenthesis v)))

(defndialect render-table
  [[alias nm]]
  (let [t (as-table-or-subquery nm)]
    (if (= alias t) t [t AS alias])))

(defndialect render-field
  [[alias nm]]
  (if (= alias nm) nm [(render-expression nm) AS alias]))

(defndialect render-fields
  [{:keys [fields tables]}]
  (if (or (nil? fields) (= fields :*))
    ASTERISK
    (comma-list (map render-field fields))))

(defndialect render-join-type
  [jt]
  (get
   {:left LEFT_OUTER_JOIN, :right RIGHT_OUTER_JOIN,
    :full FULL_OUTER_JOIN, :inner INNER_JOIN, :cross CROSS_JOIN}
   jt jt))

(defndialect render-from
  [{:keys [tables joins]}]
  (check-argument (not (empty? joins)) "No tables specified")
  [FROM
   (let [[a jn] (first joins)
         t (tables a)]
     (check-state  (contains? #{nil :cross} jn) "First join should be CROSS JOIN.")
     (render-table [a t]))
   (for [[a jn c] (rest joins) :let [t (tables a)]]
     (if (nil? jn)
       [COMMA (render-table [a t])]
       [(render-join-type jn)
        (render-table [a t])
        (if c [ON (render-expression c)] NONE)]))])

(defndialect render-where
  [{where :where}]
  (if where
    [WHERE (render-expression where)]
    NONE))

(defndialect render-order
  [{order :order}]
  (let [f (fn [[c d]]
            [(render-expression c)
             (get {nil NONE :asc ASC :desc DESC} d d)])]
    (if order
      [ORDER_BY (comma-list (map f order))]
      NONE)))

(defndialect render-modifier
  [{m :modifier}]
  (get {:distinct DISTINCT :all ALL nil NONE} m m))

(defndialect max-limit-value
  []
  Integer/MAX_VALUE)

(defndialect render-limit
  [{:keys [limit offset]}]
  (if (or limit offset)
    (let [lim (arg (if limit (int limit) (max-limit-value)))]
      [LIMIT lim
       (if offset [OFFSET (arg (int offset))] NONE)])
    NONE))

(defndialect render-group
  [{g :group}]
  (if g
    [GROUP_BY (comma-list g)]
    NONE))

(defndialect renger-having
  [{h :having}]
  (if h
    [HAVING (render-expression h)]
    NONE))

(defndialect render-select
  [relation]
  [SELECT
   (render-modifier relation)
   (render-fields relation)
   (render-from relation)
   (render-where relation)
   (render-order relation)
   (render-group relation)
   (renger-having relation)
   (render-limit relation)])

(defndialect render-delete
  [query]
  [DELETE
   (render-from query)
   (render-where query)])

(defndialect render-into
  [{t :table}]
  [INTO (qname t)])

(defn collect-fields
  [records]
  (reduce cset/union (map (comp set keys) records)))

(defndialect render-values
  [{fields :fields records :records}]
  (let [fields (if fields fields (collect-fields records))]
    (->Sql
     (:sql
      (sql*
       (parenthesis (comma-list fields))
       VALUES
       (parenthesis
        (comma-list (repeat (count fields) QMARK)))))
     (if (> (count records) 1)
       (map
        (fn [f] (with-meta (map f records) {:batch true}))
        fields)
       (map (partial get (first records)) fields)))))

(defndialect render-insert
  [query]
  [INSERT
   (render-into query)
   (render-values query)])

(defndialect render-update-fields
  [{:keys [fields]}]
  (comma-list
   (map (fn [[n c]] [SET (qname n) EQUALS (render-expression c)]) fields)))

; TODO: add joins
(defndialect render-update
  [{t :table :as query}]
  [UPDATE (render-table t)
   (render-update-fields query)
   (render-where query)])
