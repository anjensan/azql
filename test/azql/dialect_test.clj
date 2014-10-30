(ns azql.dialect-test
  (:use clojure.test)
  (:use [azql dialect core])
  (:require [clojure.java.jdbc :as jdbc])
  (:use azql.test-database))

(defndialect myfun [] :default-dialect)

(use-fixtures
  :each
  (fn [f]
    (try (f)
      (finally
        (doseq [d [::dialect-a ::dialect-b]]
          (remove-method myfun d))))))

(deftest test-parse-jdbc-url
  (is (= "postgresql" (parse-jdbc-protocol "jdbc:postgresql://localhost/db")))
  (is (= "oracle" (parse-jdbc-protocol "jdbc:oracle:thin:@myhost:1521:orcl")))
  (is (= "h2" (parse-jdbc-protocol "jdbc:h2:mem://dbname")))
  (is (= "sqlserver" (parse-jdbc-protocol "jdbc:sqlserver://localhost;ttt=1;"))))

(deftest test-current-dialect
  (is (= :azql.dialect/sql92
         (current-dialect)))
  (is (= database-dialect
         (with-azql-context database-connection (current-dialect))))
  (is (= :mydialect
         (with-bindings {#'azql.dialect/*dialect* :mydialect}
           (current-dialect))))
  (is (= :mydialect
         (with-bindings {#'azql.dialect/*dialect* :mydialect}
           (with-azql-context database-connection (current-dialect))))))

(deftest test-custom-dialect
  (register-dialect ::dialect-a)
  (register-dialect ::dialect-b ::dialect-a)
  (defmethod myfun ::dialect-a [] :dialect-a)
  (is (= :default-dialect (myfun)))
  (is (= :dialect-a (with-bindings {#'azql.dialect/*dialect* ::dialect-a} (myfun))))
  (is (= :dialect-a (with-bindings {#'azql.dialect/*dialect* ::dialect-b} (myfun)))))
