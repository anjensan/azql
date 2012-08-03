(ns azql.core-test
  (:use clojure.test
        [azql core emit]))

(use-fixtures :once (fn [f] (binding [azql.emit/quote-name identity] (f))))
              
(deftest test-simple-queries
  
  (testing "simple selects from one table"
    (are [s z] (= s (:sql (sql z)))
         
         "SELECT * FROM Table1 a"
         (select (from :a "Table1"))

         "SELECT * FROM Table1"
         (select (from "Table1"))

         "SELECT a.* FROM Table1 a"
         (select (from :a "Table1")
                 (fields [:a.*]))

         "SELECT a x, a.b Y, c FROM Table1 a"
         (select (from :a "Table1")
                 (fields* (array-map :x :a, :Y :a.b, :c :c)))))

  (testing "select from 2 tables, full join"
    (are [s z] (= s (:sql (sql z)))

         "SELECT * FROM A, B"
         (select (from "A") (from "B"))
         
         "SELECT * FROM Table1 a, Table2 b"
         (select (from :a "Table1") (from :b "Table2")))

         "SELECT * FROM A a,  B b"
         (select (from :a "A") (from :b "B"))))

(deftest test-where-clause
  (testing "simple where with one table"
    (are [s z] (= s (:sql (sql z)))

         "SELECT * FROM Table1 WHERE (id = ?)"
         (select (from "Table1") (where (= :id 10)))
         
         "SELECT * FROM Table1 WHERE ((id = ?) AND (email = ?))"
         (select (from "Table1") (where (= :id 10)) (where (= :email "x@example.com")))

         "SELECT * FROM Table1 WHERE ((id = ?) AND (? <> fi) AND (email = ?))"
         (select
          (from "Table1")
          (where (and (= :id 10) (not= 2 :fi)))
          (where (= :email "x@example.com")))
         )))

(deftest test-complex-fields
  (testing "select expression"
    (are [s z] (= s (:sql (sql z)))
         
         "SELECT (a + b) c FROM Table"
         (select (from "Table") (fields {:c (+ :a :b)}))

         "SELECT (a * b * c) z FROM Table"
         (select (from "Table") (fields {:z (* :a :b :c)}))

         )))

(deftest test-joins
  (testing "test cross join"
    (are [s z] (= s (:sql (sql z)))
         
         "SELECT * FROM A a CROSS JOIN B b"
         (select
          (from :a "A")
          (join-cross :b "B"))
         
         "SELECT * FROM A a CROSS JOIN B b CROSS JOIN C c"
         (select
          (join-cross :a "A")
          (join-cross :b "B")
          (join-cross :c "C"))

         "SELECT * FROM A a CUSTOM JOIN B b"
         (select
          (from :a "A")
          (join* (raw "CUSTOM JOIN") :b "B" nil))
         ))
  
  (testing "test inner joins"
    (are [s z] (= s (:sql (sql z)))

         "SELECT * FROM A a INNER JOIN B b ON (a.x = b.y)"
         (select
          (from :a "A")
          (join :b "B" (= :a.x :b.y))))
    ))

(deftest test-orderby
  (testing "test simple orderby"
    (are [s z] (= s (:sql (sql z)))
         "SELECT * FROM A a ORDER BY x"
         (select (from :a "A") (order :x))
         "SELECT * FROM A a ORDER BY x ASC"
         (select (from :a "A") (order :x :asc))
         "SELECT * FROM A a ORDER BY x DESC"
         (select (from :a "A") (order :x :desc))
         "SELECT * FROM A a ORDER BY a.x, a.y ASC, a.z DESC"
         (select (from :a "A") (order :a.x) (order :a.y :asc) (order :a.z :desc)))))
