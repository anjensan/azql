(ns azql.dialect-test
  (:use clojure.test)
  (:use azql.dialect)
  (:require [clojure.java.jdbc :as jdbc])
  (:use azql.test-database))

(defndialect myfun [] :default-dialect)

(use-fixtures
  :each
  (fn [f]
    (try (f)
      (finally
        (remove-method myfun ::dialect-a)))))

(deftest test-parse-jdbc-url
  (is (= :postgresql (parse-jdbc-protocol "jdbc:postgresql://localhost/db")))
  (is (= :oracle (parse-jdbc-protocol "jdbc:oracle:thin:@myhost:1521:orcl")))
  (is (= :h2 (parse-jdbc-protocol "jdbc:h2:mem://dbname")))
  (is (= :sqlserver (parse-jdbc-protocol "jdbc:sqlserver://localhost;ttt=1;"))))

(deftest test-current-dialect
  (is (= :azql.dialect/sql92
         (current-dialect)))
  (is (= database-dialect
         (jdbc/with-connection database-connection (current-dialect))))
  (is (= :mydialect
         (binding [*dialect* :mydialect]
           (current-dialect))))
  (is (= :mydialect
         (binding [*dialect* :mydialect]
           (jdbc/with-connection database-connection (current-dialect))))))

(deftest test-custom-dialect
  (register-dialect ::dialect-a)
  (register-dialect ::dialect-b ::dialect-a)
  (defmethod myfun ::dialect-a [] :dialect-a)
  (is (= :default-dialect (myfun)))
  (is (= :dialect-a (binding [*dialect* ::dialect-a] (myfun))))
  (is (= :dialect-a (binding [*dialect* ::dialect-b] (myfun)))))
