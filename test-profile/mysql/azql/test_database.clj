(ns azql.test-database
  (:use azql.dialect)
  (:use azql.emit)
  (:require [clojure.java.jdbc :as jdbc]))

(def database-connection
  {:classname "com.mysql.jdbc.Driver"
   :subprotocol "mysql"
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

(def database-dialect ::mysql)
(register-dialect ::mysql)

(defmethod guess-dialect :mysql [_]
  ::mysql)

(defmethod quote-name ::mysql [x]
  (str \` x \`))

(def database-quote-symbol \`)
