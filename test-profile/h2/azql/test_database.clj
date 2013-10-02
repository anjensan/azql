(ns azql.test-database
  (:use [azql core dialect emit]))

(def database-connection
  {:classname "org.h2.Driver"
   :subprotocol "h2"
   :subname "mem://azql_test"})

(defn- do-commands
  [db & commands]
  (doseq [c commands]
    (execute! db (raw c))))

(defn create-database
  [db]
  (do-commands db

   "CREATE TABLE users (
      id INT PRIMARY KEY AUTO_INCREMENT,
      name VARCHAR(50),
      dob DATE
    )"

   "CREATE TABLE posts (
      id INT PRIMARY KEY AUTO_INCREMENT,
      text VARCHAR(10000),
      userid INT
    )"

   "CREATE TABLE comments (
      id INT PRIMARY KEY AUTO_INCREMENT,
      text VARCHAR(500),
      userid INT,
      postid INT,
      parentid INT
    )"
   ))

(def database-dialect ::h2)
(register-dialect ::h2)
(defmethod guess-dialect :h2 [_] ::h2)

(defmethod quote-name ::h2 [x]
  (str x))

