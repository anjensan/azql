(ns azql.dialect-test
  (:use clojure.test)
  (:use azql.dialect)
  (:require [clojure.java.jdbc :as jdbc]))

(def h2-database-connection
  {:classname   "org.h2.Driver"
   :subprotocol "h2"
   :subname "mem://azql-test"})

(defndialect myfun [] :default-dialect)

(use-fixtures
  :each
  (fn [f]
    (try (f)
      (finally
        (remove-method guess-dialect :h2)
        (remove-method myfun ::dialect-a)))))

(deftest test-parse-jdbc-url
  (is (= :postgresql (parse-jdbc-protocol "jdbc:postgresql://localhost/db")))
  (is (= :oracle (parse-jdbc-protocol "jdbc:oracle:thin:@myhost:1521:orcl")))
  (is (= :h2 (parse-jdbc-protocol "jdbc:h2:mem://dbname")))
  (is (= :sqlserver (parse-jdbc-protocol "jdbc:sqlserver://localhost;ttt=1;"))))

(deftest test-current-dialect
  (is (= :azql.dialect/sql92
         (current-dialect)))
  (is (= :azql.dialect/sql92
         (jdbc/with-connection h2-database-connection (current-dialect))))
  (is (= :mydialect
         (binding [*dialect* :mydialect]
           (current-dialect))))
  (is (= :mydialect
         (binding [*dialect* :mydialect]
           (jdbc/with-connection h2-database-connection (current-dialect)))))
  (testing "define new dialect"
           (defmethod guess-dialect :h2 [_] ::h2-dialect)
           (is (= ::h2-dialect (jdbc/with-connection h2-database-connection (current-dialect))))))

(deftest test-custom-dialect
  (derive ::dialect-a :azql.dialect/sql92)
  (derive ::dialect-b ::dialect-a)
  (defmethod myfun ::dialect-a [] :dialect-a)
  (is (= :default-dialect (myfun)))
  (is (= :dialect-a (binding [*dialect* ::dialect-a] (myfun))))
  (is (= :dialect-a (binding [*dialect* ::dialect-b] (myfun)))))
