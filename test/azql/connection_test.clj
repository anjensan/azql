(ns azql.connection-test
  (:use azql.connection)
  (:use clojure.test)
  (:require [clojure.java.jdbc :as jdbc]))

(def h2-database-connection
  {:classname   "org.h2.Driver"
   :subprotocol "h2"
   :subname "mem://azql-test"})

(use-fixtures :each #(try (%) (finally (close-global-connection))))

(deftest test-global-connection
  (open-global-connection h2-database-connection)
  (is (not (jdbc/find-connection)))
  (is (with-global-connection (jdbc/find-connection)))
  (close-global-connection)
  (is (thrown? IllegalStateException (with-global-connection nil))))

(deftest test-ignore-scoped-connection
  (open-global-connection h2-database-connection)
  (is
   (not
    (identical?
     (with-global-connection (jdbc/find-connection))
     (jdbc/with-connection h2-database-connection (jdbc/find-connection))))))
