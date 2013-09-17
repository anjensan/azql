(ns azql.test-database
  (:use azql.dialect)
  (:require [clojure.java.jdbc :as jdbc]))

(def database-connection
  {:classname "org.postgresql.Driver"
   :subprotocol "postgresql"
   :user "test"
   :password "test"
   :subname "//localhost/azql_test"})

(defn create-database
  []

  (jdbc/do-commands "DROP TABLE IF EXISTS users")
  (jdbc/do-commands "DROP TABLE IF EXISTS posts")
  (jdbc/do-commands "DROP TABLE IF EXISTS comments")

  (jdbc/create-table
    :users
    [:id "SERIAL" "PRIMARY KEY"]
    [:name "VARCHAR(50)"]
    [:dob "DATE"])

  (jdbc/create-table
    :posts
    [:id "SERIAL" "PRIMARY KEY"]
    [:text "VARCHAR(10000)"]
    [:userid "INT"])

  (jdbc/create-table
    :comments
    [:id "SERIAL" "PRIMARY KEY"]
    [:text "VARCHAR(500)"]
    [:userid "INT"]
    [:postid "INT"]
    [:parentid "INT"]))

(def database-dialect ::pgsql)
(register-dialect ::pgsql)

(defmethod guess-dialect :postgresql [_] ::pgsql)

(def database-quote-symbol \")
