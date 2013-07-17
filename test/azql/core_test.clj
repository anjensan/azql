(ns azql.core-test
  (:use clojure.test [azql emit dialect expression core])
  (:require [clojure.java.jdbc :as jdbc]))

;; custom dialect
(register-dialect ::dialect)
(deffunctions ::dialect fun sin cos)
(defmethod entity-naming-strategy ::dialect [] str)
(use-fixtures :once (fn [f] (binding [azql.dialect/*dialect* ::dialect] (f))))

(deftest test-simple-queries
  (testing
    "simple selects from one table"
    (are [s z] (= s (:sql (sql* z)))
         "SELECT * FROM X"
         (table :X)
         "SELECT * FROM X"
         (select (from :X))
         "SELECT * FROM Table1 AS a"
         (select (from :a "Table1"))
         "SELECT * FROM Table1 AS a"
         (select (from :a (table "Table1")))
         "SELECT DISTINCT * FROM Table1 AS a"
         (select (modifier :distinct) (from :a "Table1"))
         "SELECT * FROM Table1"
         (select (from "Table1"))
         "SELECT a.* FROM Table1 AS a"
         (select (from :a "Table1") (fields [:a.*]))
         "SELECT a AS x, a.b AS Y, c FROM Table1 AS a"
         (select (from :a "Table1")
                 (fields* (array-map :x :a, :Y :a.b, :c :c)))
         "SELECT *, a, b FROM X"
         (select (from :X) (fields [:* :a :b]))
         "SELECT a, b FROM A"
         (select [:a :b] (from :A))))

  (testing
    "select from 2 tables, full join"
    (are [s z] (= s (:sql (sql*  z)))
         "SELECT * FROM A, B"
         (select (from :A) (from :B))
         "SELECT * FROM A AS a, B"
         (select (from :a :A) (from :B))
         "SELECT * FROM Table1 AS a, Table2 AS b"
         (select (from :a :Table1) (from :b :Table2))
         "SELECT * FROM A AS a, B AS b"
         (select (from :a "A") (from :b "B"))
         "SELECT X.*, Y.a FROM X, Y"
         (select (from :X) (from :Y) (fields [:X.* :Y.a]))
         "SELECT x.x, y.* FROM X AS x, Y AS y"
         (select (from :x :X) (from :y :Y) (fields [:x.x :y.*]))
         "SELECT * FROM A, B, C, D"
         (select (from :A) (from :B) (from :C) (from :D)))))

(deftest test-where-clause

  (testing
    "simple where with one table"
    (are [s z] (= s (:sql (sql*  z)))
         "SELECT * FROM Table1 WHERE (id = ?)"
         (select (from "Table1") (where (= :id 10)))
         "SELECT * FROM Table1 WHERE ((id = ?) AND (email = ?))"
         (select
           (from "Table1")
           (where (= :id 10))
           (where (= :email "x@example.com")))
         "SELECT * FROM Table1 WHERE ((id = ?) AND (? <> fi) AND (email = ?))"
         (select
           (from "Table1")
           (where (and (= :id 10) (not= 2 :fi)))
           (where (= :email "x@example.com")))))

  (testing
    "where with aliases"
    (are [s z] (= s (:sql (sql*  z)))
         "SELECT * FROM A AS a, B AS b WHERE (a.x = b.y)"
         (select (from :a :A) (from :b :B) (where (= :a.x :b.y)))
         "SELECT * FROM A, B, C WHERE ((A = a) AND ((A.x + C.y) > B.z))"
         (select (from :A) (from :B) (from :C)
                 (where (and (= :A :a) (> (+ :A.x :C.y) :B.z)))))))

(deftest test-complex-fields
  (testing
    "select expression"
    (deffunctions sin)
    (are [s z] (= s (:sql (sql* z)))
         "SELECT (a + b) AS c FROM Table"
         (select (from "Table") (fields {:c (+ :a :b)}))
         "SELECT (a * b * c) AS z FROM Table"
         (select (from "Table") (fields {:z (* :a :b :c)}))
         "SELECT a, b AS c, sin(d) AS s FROM X"
         (select (from :X) (fields {:a :a :c :b :s (sin :d)})))))

(deftest test-joins
  (testing
    "cross join"
    (are [s z] (= s (:sql (sql*  z)))
         "SELECT * FROM A AS a CROSS JOIN B AS b"
         (select
           (from :a "A")
           (join-cross :b "B"))
         "SELECT * FROM A AS a CROSS JOIN B AS b CROSS JOIN C AS c"
         (select
           (join-cross :a "A")
           (join-cross :b "B")
           (join-cross :c "C"))))

  (testing
    "custom join"
    "SELECT * FROM A AS a COOL JOIN B AS b"
    (select
      (from :a "A")
      (join* (raw "COOL JOIN") :b "B" nil)))

  (testing
    "inner joins"
    (are [s z] (= s (:sql (sql* z)))
         "SELECT * FROM A AS a INNER JOIN B AS b ON (a.x = b.y)"
         (select
           (from :a "A")
           (join :b "B" (= :a.x :b.y)))))

  (testing
    "outer joins"
    (are [s z] (= s (:sql (sql* z)))
         "SELECT * FROM A LEFT OUTER JOIN B ON (x = y)"
         (select (from :A) (join-left :B (= :x :y)))
         "SELECT * FROM A LEFT OUTER JOIN B AS b ON (x = y)"
         (select (from :A) (join-left :b :B (= :x :y)))
         "SELECT * FROM A RIGHT OUTER JOIN B ON (x = y)"
         (select (from :A) (join-right :B (= :x :y)))
         "SELECT * FROM A RIGHT OUTER JOIN B AS b ON (x = b.y)"
         (select (from :A) (join-right :b :B (= :x :b.y)))
         "SELECT * FROM A FULL OUTER JOIN B ON (A.x = B.y)"
         (select (from :A) (join-full :B (= :A.x :B.y)))
         "SELECT * FROM A FULL OUTER JOIN B AS b ON (A.x = b.y)"
         (select (from :A) (join-full :b :B (= :A.x :b.y))))))

(deftest test-order-by

  (testing
    "simple orderby"
    (are [s z] (= s (:sql (sql* z)))
         "SELECT * FROM A AS a ORDER BY x"
         (select (from :a "A") (order :x))
         "SELECT * FROM A AS a ORDER BY x ASC"
         (select (from :a "A") (order :x :asc))
         "SELECT * FROM A AS a ORDER BY x DESC"
         (select (from :a "A") (order :x :desc))
         "SELECT * FROM A AS a ORDER BY a.x, a.y ASC, a.z DESC"
         (select (from :a "A") (order :a.z :desc) (order :a.y :asc) (order :a.x))))

  (testing
    "order by expression"
    (are [s z] (= s (:sql (sql* z)))
         "SELECT * FROM A ORDER BY (x + y)"
         (select (from :A) (order (+ :x :y)))
         "SELECT * FROM A ORDER BY cos(sin(x)) DESC"
         (select (from :A) (order (cos (sin :x)) :desc))
         "SELECT * FROM A ORDER BY fun(x) DESC, (y + ?) ASC, z DESC"
         (select (from :A) (order :z :desc) (order (+ :y 1) :asc) (order (fun :x) :desc)))))

(deftest test-limit-and-offset
  (testing
    "offset & limit"
    (are [s z] (= s (:sql (sql* z)))
         "SELECT * FROM X LIMIT ?"
         (select (from "X") (limit 10))
         "SELECT * FROM X LIMIT ? OFFSET ?"
         (select (from "X") (limit 10) (offset 20)))))

(deftest test-group-by

  (testing
    "simple grouping"
    (are [s z] (= s (:sql (sql* z)))
         "SELECT a FROM T GROUP BY a"
         (select (from "T") (group [:a]) (fields [:a]))
         "SELECT a, b FROM T GROUP BY a, b"
         (select (from "T") (group [:a :b]) (fields [:a :b]))))

  (testing
    "grouping with having on"
    (are [s z] (= s (:sql (sql* z)))
         "SELECT * FROM T GROUP BY a HAVING (a > ?)"
         (select (from "T") (group [:a]) (having (> :a 1)))
         "SELECT a, b FROM T GROUP BY a, b HAVING ((a > ?) AND (? < b))"
         (select (from "T") (group [:a, :b]) (fields [:a :b])
                 (having (> :a 1)) (having (< 2 :b))))))

(deftest test-subqueries
  (testing
    "subqueries in joins"
    (are [s z] (= s (:sql (sql* z)))
         "SELECT * FROM A"
         (select (from (table :A)))
         "SELECT * FROM (SELECT * FROM A) AS a"
         (select (from :a (select (from :A))))
         "SELECT x FROM (SELECT x FROM A) AS a"
         (select (from :a (select (fields [:x]) (from :A))) (fields [:x]))
         "SELECT * FROM A, (SELECT * FROM B) AS b"
         (select (from :A) (from :b (select (from :B))))
         "SELECT * FROM (SELECT * FROM A) AS a INNER JOIN (SELECT * FROM B) AS b ON (x = y)"
         (select
           (from :a (select (from :A)))
           (join :b (select (from :B)) (= :x :y)))
         "SELECT (SELECT b FROM B WHERE (a = c)) FROM A"
         (select [(select [:b] (from :B) (where {:a :c}))] (from :A))
         "SELECT a, (SELECT b FROM B WHERE (a = c)) AS c FROM A"
         (select {:a :a, :c (select [:b] (from :B) (where {:a :c}))} (from :A)))
    (is (re-matches
          #"SELECT \* FROM \(SELECT x FROM A\) AS __\d+"
          (:sql (sql* (select (from (select [:x] (from :A)))))))))

  (testing
    "operators `exists`, `any`, `all`"
    (are [s z] (= s (:sql (sql* z)))
         "SELECT * FROM A WHERE (EXISTS (SELECT * FROM B WHERE (x = y)))"
         (select (from :A) (where (exists? (select (from :B) (where (= :x :y))))))
         "SELECT * FROM A WHERE (NOT EXISTS (SELECT * FROM B WHERE (A.x = B.x)))"
         (select (from :A) (where (not-exists? (select (from :B) (where (= :A.x :B.x))))))
         "SELECT * FROM A WHERE (x IN (SELECT y FROM B))"
         (select (from :A) (where (in? :x (select (from :B) (fields [:y])))))
         "SELECT * FROM A WHERE (z < ANY (SELECT g FROM B))"
         (select (from :A) (where (< :z (any (select (fields [:g]) (from :B))))))
         "SELECT * FROM A WHERE (p > SOME (SELECT t FROM B))"
         (select (from :A) (where (> :p (some :t (select (from :B))))))
         "SELECT * FROM A WHERE (z <> ALL (SELECT t FROM X WHERE (x <> ?)))"
         (select (from :A) (where (<> :z (all :t (select (from :X) (where (<> :x 1))))))))))

(deftest test-defselect
  (testing
    "defselect azql queries"

    (defselect dst-select [] (from :X))
    (is (= "SELECT * FROM X" (:sql (sql* (dst-select)))))
    (is (= [] (:args (sql* (dst-select)))))

    (defselect dst-select [x y] (from :X) (where (and (= y :y) (= :x x))))
    (is (= "SELECT * FROM X WHERE ((? = y) AND (x = ?))" (:sql (sql* (dst-select 1 2)))))
    (is (= [2 1] (:args (sql* (dst-select 1 2)))))

    (defselect ^:x dst-select "doc" [a _] (from :X) (limit a))
    (is (= "SELECT * FROM X LIMIT ?" (:sql (sql* (dst-select 1 2)))))
    (is (= [1] (:args (sql* (dst-select 1 2)))))
    (is (= {:doc "doc" :x true} (select-keys (meta #'dst-select) #{:doc :x}))))

  (testing
    "defselect with dialects"

    (defselect dst-select [] (from :X))
    (is (= "SELECT * FROM X"
           (:sql
            (binding [azql.dialect/*dialect* ::dialect]
              (sql* (dst-select))))))
    (is (= "SELECT * FROM \"X\""
           (:sql
            (binding [azql.dialect/*dialect* default-dialect]
              (sql* (dst-select)))))))

  (testing
    "defselect with raw queries"

    (defselect dst-select [] "select")
    (is (= "select" (:sql (sql* (dst-select)))))
    (is (= [] (:args (sql* (dst-select)))))

    (defselect dst-select [x y] "SELECT * FROM X WHERE :y = y AND x = :x")
    (is (= "SELECT * FROM X WHERE ? = y AND x = ?" (:sql (sql* (dst-select 1 2)))))
    (is (= [2 1] (:args (sql* (dst-select 1 2)))))

    (defselect ^:x dst-select "doc" [a b c] ":c:a:b")
    (is (= "???" (:sql (sql* (dst-select 1 2 3)))))
    (is (= [3 1 2] (:args (sql* (dst-select 1 2 3)))))
    (is (= {:doc "doc" :x true} (select-keys (meta #'dst-select) #{:doc :x})))))

(def single-table-select? #'azql.core/single-table-select?)

(deftest test-select-from-single-table
  (is (single-table-select? (table :X)))
  (is (single-table-select? (table "X")))
  (is (not (single-table-select? (select (from :X)))))
  (is (not (single-table-select? (select (from "X")))))
  (is (not (single-table-select? (-> (table :X) (order :X)))))
  (is (not (single-table-select? (-> (table :X) (join-cross :T)))))
  (is (not (single-table-select? (-> (table "X") (limit 100))))))

(deftest test-composed-queries

  (testing
    "union"
    (are [s z] (= s (:sql (sql* z)))
         "SELECT * FROM (SELECT a FROM T)"
         (union (select [:a] (from "T")))
         "SELECT * FROM (SELECT a FROM T) ORDER BY a"
         (union (order :a) (select [:a] (from "T")))
         "(SELECT a FROM A) UNION (SELECT a FROM B)"
         (union (select [:a] (from :A)) (select [:a] (from :B)))
         "(SELECT a FROM A) UNION ALL (SELECT a FROM B)"
         (union (select [:a] (from :A)) (modifier :all) (select [:a] (from :B)))
         "(SELECT a FROM A) UNION (SELECT a FROM B) ORDER BY a"
         (union (select [:a] (from :A)) (select [:a] (from :B)) (order :a))
         "((SELECT * FROM A) UNION (SELECT * FROM B)) UNION ALL (SELECT * FROM C)"
         (union (union (table :A) (table :B)) (modifier :all) (table :C))
         "(SELECT * FROM A) UNION ((SELECT * FROM B) UNION ALL (SELECT * FROM C))"
         (union (table :A) (union (table :B) (modifier :all) (table :C)))))

  (testing
    "intersect"
    (are [s z] (= s (:sql (sql* z)))
         "SELECT * FROM (SELECT a FROM T)"
         (intersect (select [:a] (from "T")))
         "(SELECT a FROM A) INTERSECT (SELECT a FROM B)"
         (intersect (select [:a] (from :A)) (select [:a] (from :B)))))

  (testing
    "except"
    (are [s z] (= s (:sql (sql* z)))
         "SELECT * FROM (SELECT a FROM T)"
         (except (select [:a] (from "T")))
         "(SELECT a FROM A) EXCEPT (SELECT a FROM B)"
         (except (select [:a] (from :A)) (select [:a] (from :B))))))

(deftest test-delete
  (testing
    "delete statement"
    (are [s z] (= s (:sql (sql* z)))
         "DELETE FROM X"
         (-> (delete*) (from :X))
         "DELETE FROM X WHERE (id = ?)"
         (-> (delete*) (from :X) (where {:id 1}))
         "DELETE FROM X INNER JOIN Y ON (a = b) WHERE (id = ?)"
         (-> (delete*) (from :X) (join :Y {:a :b}) (where {:id 1}))
         "DELETE FROM X AS x INNER JOIN Y AS y ON (x.a = y.b) WHERE (id = ?)"
         (-> (delete*) (from :x :X) (join :y :Y {:x.a :y.b}) (where {:id 1})))))

(deftest test-update
  (testing
    "update statement"
    (are [s z] (= s (:sql (sql* z)))
         "UPDATE X SET a = ? WHERE (id = ?)"
         (-> (update* :X) (setf :a 1) (where {:id 1}))
         "UPDATE X SET a = b, SET b = a WHERE (id = ?)"
         (-> (update* :X) (setf :a :b) (setf :b :a) (where {:id 1}))
         "UPDATE X SET c = (c + ?) WHERE (id IN (?, ?, ?))"
         (-> (update* :X) (setf :c (+ :c 1)) (where (in? :id [1 2 3])))
         "UPDATE X SET x = (SELECT t FROM T WHERE (T.a = X.b))"
         (-> (update* :X) (setf :x (select [:t] (from :T) (where {:T.a :X.b})))))))

(deftest test-insert
  (testing
    "insert statement"
    (are [s z] (= s (:sql (sql* z)))
         "INSERT INTO X (a, b) VALUES (?, ?)"
         (-> (insert* :X) (values {:a 1, :b 2}))
         "INSERT INTO X (a, b) VALUES (?, ?)"
         (-> (insert* :X) (values [{:a 1, :b 2}, {:a 3, :b 4}]))
         "INSERT INTO X (a, b) VALUES (?, ?)"
         (-> (insert* :X) (values {:a 1, :b 2}) (values {:a 3, :b 4}))
         "INSERT INTO X (a, b) VALUES (?, ?)"
         (-> (insert* :X) (values {:a 1, :b 2}) (values {})))
    (are [s z] (= s (:args (sql* z)))
         [[1 3] [2 4]]
         (-> (insert* :X) (values [{:a 1, :b 2}, {:a 3, :b 4}]))
         [[1 3] [2 4]]
         (-> (insert* :X) (values {:a 1, :b 2}) (values {:a 3, :b 4}))
         [[1 nil] [nil 4]]
         (-> (insert* :X) (values {:a 1}) (values {:b 4}))
         [[nil 1] [nil 2]]
         (-> (insert* :X) (values [{} {:a 1 :b 2}])))))
