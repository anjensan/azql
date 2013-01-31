(ns azql.test-database
  (:use azql.dialect)
  (:use azql.emit)
  (:require [clojure.java.jdbc :as jdbc]))

(def database-connection
  {:classname "com.mysql.jdbc.Driver"
   :subprotocol "mysql"
   :user "test"
   :password "test"
   :subname "//localhost:3306/azql_test"})

(defn create-database
  []
  (jdbc/drop-table :users)
  (jdbc/drop-table :posts)
  (jdbc/drop-table :comments)
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

(defmethod entity-naming-strategy ::mysql []
  (fn [x] (str \` x \`)))

(def database-quote-symbol \`)