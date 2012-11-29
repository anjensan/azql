(ns azql.render
  (:use [azql util dialect emit expression])
  (:require [clojure.set :as cset])
  (:require [clojure.java.jdbc :as jdbc]))

(defprotocol Query
  (_dummy "Dummy method, see #CLJ-966" [_]))

(ns-unmap *ns* '_dummy)

(defn- as-table-or-subquery
  [v]
  (cond
   (keyword? v) v
   (string? v) (keyword v)
   :else (compose-sql (parentheses v))))

(defn- sql-atom?
  [x]
  (not (or (sql? x) (satisfies? Query x))))

(defndialect render-expression-or-subselect
  [e]
  (let [r (render-expression e)]
    (if (sql-atom? e) r (parentheses r))))

(defndialect render-table-alias?
  [alias table]
  (not= alias table))

(defndialect render-field-alias?
  [alias field]
  (not
    (or
      (= alias field)
      (surrogate-alias? alias))))

(defndialect render-table
  [[alias table]]
  (let [t (as-table-or-subquery table)]
    (if (render-table-alias? alias t) (compose-sql t AS alias) t)))

(defndialect render-field
  [[alias nm]]
  (let [e (render-expression-or-subselect nm)]
    (if (render-field-alias? alias nm) (compose-sql e AS alias) e)))

(defndialect render-fields
  [{:keys [fields]}]
  (compose-sql
    (if (or (nil? fields) (= fields :*))
      ASTERISK
      (comma-list (map render-field fields)))))

(defndialect render-join-type
  [jt]
  (get
    {:left LEFT_OUTER_JOIN, :right RIGHT_OUTER_JOIN,
     :full FULL_OUTER_JOIN, :inner INNER_JOIN, :cross CROSS_JOIN}
    jt jt))

(defndialect render-from
  [{:keys [tables joins]}]
  (check-argument (not (empty? joins)) "No tables specified")
  (compose-sql
    FROM
    (let [[a jn] (first joins)
          t (tables a)]
      (check-state (contains? #{nil :cross} jn) "First join should be CROSS JOIN.")
      (render-table [a t]))
    (compose-sql*
      (for [[a jn c] (rest joins) :let [t (tables a)]]
        (if (nil? jn)
          (compose-sql NOSP COMMA (render-table [a t]))
          (compose-sql
            (render-join-type jn)
            (render-table [a t])
            (if c
              (compose-sql ON (render-expression c))
              NONE)))))))

(defndialect render-where
  [{where :where}]
  (if where
    (compose-sql WHERE (render-expression where))
    NONE))

(defndialect render-order
  [{order :order}]
  (let [f (fn [[c d]]
            (compose-sql
              (render-expression c)
              (get {nil NONE :asc ASC :desc DESC} d d)))]
    (if order
      (compose-sql ORDER_BY (comma-list (map f order)))
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
    (let [lim (arg (if limit limit (max-limit-value)))]
      (compose-sql
        LIMIT lim
        (if offset (compose-sql OFFSET offset) NONE)))
    NONE))

(defndialect render-group
  [{g :group}]
  (if g
    (compose-sql GROUP_BY (comma-list g))
    NONE))

(defndialect renger-having
  [{h :having}]
  (if h
    (compose-sql HAVING (render-expression h))
    NONE))

(defndialect render-select
  [relation]
  (compose-sql
    SELECT
    (render-modifier relation)
    (render-fields relation)
    (render-from relation)
    (render-where relation)
    (render-order relation)
    (render-group relation)
    (renger-having relation)
    (render-limit relation)))

(defndialect render-delete
  [query]
  (compose-sql
    DELETE
    (render-from query)
    (render-where query)))

(defndialect render-into
  [{t :table}]
  (compose-sql
    INTO (qname t)))

(defn collect-fields
  [records]
  (reduce cset/union (map (comp set keys) records)))

(defndialect render-values
  [{fields :fields records :records}]
  (let [fields (if fields fields (collect-fields records))]
    (->Sql
      (:sql
        (sql*
          (parentheses (comma-list fields))
          VALUES
          (parentheses
            (comma-list (repeat (count fields) QMARK)))))
      (if (> (count records) 1)
        (map
          (fn [f] (with-meta (map f records) {:batch true}))
          fields)
        (map (partial get (first records)) fields)))))

(defndialect render-insert
  [query]
  (compose-sql
    INSERT
    (render-into query)
    (render-values query)))

(defndialect render-update-fields
  [{:keys [fields]}]
  (comma-list
    (map (fn [[n c]] (compose-sql SET (qname n) EQUALS (render-expression-or-subselect c))) (reverse fields))))

(defndialect render-update
  [{t :table :as query}]
  (compose-sql
    UPDATE (render-table t)
    (render-update-fields query)
    (render-where query)))

(defndialect render-combine-type
  [{ct :type m :modifier}]
  (compose-sql
    (get {:union UNION, :intersect INTERSECT, :except EXCEPT} ct ct)
    (get {:all ALL, :distinct NONE, nil NONE} m m)))

(defndialect render-combined
  [{q :queries :as query}]
  (check-argument (not (empty? q)) "No queries specified")
  (compose-sql
    (if (== 1 (count q))
      (compose-sql SELECT '* FROM (parentheses (first q)))
      (compose-sql
        (compose-sql*
          (interpose
            (render-combine-type query)
            (map parentheses q)))))
    (render-order query)
    (render-limit query)))
