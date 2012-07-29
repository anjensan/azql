(ns azql.emit-test
  (:use clojure.test)
  (:use azql.emit))

(deftest test-qnames
  (testing "parse qnames"
    (is (= [:a :b] (parse-qname :a.b)))
    (is (= [:a] (parse-qname :a)))
    (is (= [:a :b :c] (parse-qname :a.b.c)))
    (is (= [:a :b] (parse-qname [:a :b]))))
  (testing "emit qnames"
    (is (= "\"A\".\"B\"" (emit-qname :A.B)))
    (is (= "\"Abc\"" (emit-qname :Abc)))))

(deftest test-sql
  (testing "test sql generation"
    (is (= (->Sql "SELECT * FROM \"Table\"" ())
           (sql [(raw "SELECT") :* (raw "FROM") :Table])))
    (is (= (->Sql "A B C D" ())
           (sql [[(raw "A") [(raw "B")]] [[[]]] [(raw "C")] (raw "D")])))
    (is (= (->Sql "ABC" ())
           (sql (raw "ABC"))))
    (is (= (->Sql "\"A\"" ())
           (sql :A)))
    (is (= (->Sql "?" [123])
           (sql 123)))
    (is (= (->Sql "A ? B ?" ["a" "b"])
           (sql [(raw "A") "a" (raw "B") "b"])))))
    

    

  

