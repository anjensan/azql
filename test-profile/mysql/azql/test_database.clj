(ns azql.test-database
  (:use [azql core dialect emit]))

(def database-connection
  {:classname "com.mysql.jdbc.Driver"
   :subprotocol "mysql"
   :user "test"
   :password "test"
   :subname "//localhost/azql_test"})

(defn- do-commands
  [db & commands]
  (doseq [c commands]
    (execute! db (format-sql c))))

(defn create-database
  [db]
  (do-commands db

   "DROP TABLE IF EXISTS users"
   "CREATE TABLE users (
      id INT PRIMARY KEY AUTO_INCREMENT,
      name VARCHAR(50),
      dob DATE
    )"

   "DROP TABLE IF EXISTS posts"
   "CREATE TABLE posts (
      id INT PRIMARY KEY AUTO_INCREMENT,
      text VARCHAR(10000),
      userid INT
    )"

   "DROP TABLE IF EXISTS comments"
   "CREATE TABLE comments (
      id INT PRIMARY KEY AUTO_INCREMENT,
      text VARCHAR(500),
      userid INT,
      postid INT,
      parentid INT
    )"
   ))

(def database-dialect ::mysql)
(register-dialect ::mysql)

(defmethod guess-dialect :mysql [_] ::mysql)

(defmethod quote-name ::mysql [x]
  (str \` x \`))

(def database-quote-symbol \`)
