(ns azql.emit-test
  (:use clojure.test)
  (:use azql.emit))

(deftest test-qnames
  (testing "parse qnames"
    (is (= [:a :b] (parse-qname :a.b)))
    (is (= [:a] (parse-qname :a)))
    (is (= [:a :b :c] (parse-qname :a.b.c))))
  (testing "emit qnames"
    (is (= "\"A\".\"B\"" (emit-qname :A.B)))
    (is (= "\"Abc\"" (emit-qname :Abc)))))

(deftest test-sql
  (testing "test sql generation"
    (is (= (->Sql "SELECT * FROM \"Table\"" ())
           (sql [(raw "SELECT") :* (raw "FROM") :Table])))
    (is (= (->Sql "A  a B C D" ())
           (sql [[(raw "A  a") [(raw "B")]] [[[]]] [(raw "C")] (raw "D")])))
    (is (= (->Sql "A ( () )  ,  BC" ())
           (sql (raw "A ( () )  ,  BC"))))
    (is (= (->Sql "\"A\"" ())
           (sql :A)))
    (is (= (->Sql "A B" ())
           (sql [(raw "A") NONE NONE NONE NONE (raw "B")])))
    (is (= (->Sql "A, B, C" ())
           (sql [(raw "A") NONE COMMA NONE (raw "B")
                 NONE NONE COMMA NONE NONE (raw "C")])))
    (is (= (->Sql "?" [123])
           (sql 123)))
    (is (= (->Sql "A ? B ?" ["a" "b"])
           (sql [(raw "A") "a" (raw "B") "b"])))
    (is (= (->Sql "?" [nil]) (sql [nil])))))
    

    

  

