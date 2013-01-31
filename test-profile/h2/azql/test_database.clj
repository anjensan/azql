(ns azql.test-database
  (:use azql.dialect)
  (:require [clojure.java.jdbc :as jdbc]))

(def database-connection
  {:classname "org.h2.Driver"
   :subprotocol "h2"
   :subname "mem://azql_test"})

(defn create-database
  []
  (jdbc/create-table
    :users
    [:id "INT" "PRIMARY KEY" "AUTO_INCREMENT"]
    [:name "VARCHAR(50)"]
    [:dob "DATE"])
  (jdbc/create-table
    :posts
    [:id "INT" "PRIMARY KEY" "AUTO_INCREMENT"]
    [:text "VARCHAR(10000)"]
    [:userid "INT"])
  (jdbc/create-table
    :comments
    [:id "INT" "PRIMARY KEY" "AUTO_INCREMENT"]
    [:text "VARCHAR(500)"]
    [:userid "INT"]
    [:postid "INT"]
    [:parentid "INT"]))

(def database-dialect ::h2)
(register-dialect ::h2)
(defmethod guess-dialect :h2 [_] ::h2)

(def database-quote-symbol \")