(ns azql.test-database
  (:use [azql core dialect emit]))

(def database-connection
  {:classname "com.mysql.jdbc.Driver"
   :subprotocol "mysql"
   :user "test"
   :password "test"
   :subname "//localhost/azql_test"
   :dialect ::my-mysql})

(defn- do-commands
  [db & commands]
  (doseq [c commands]
    (execute! db (sql c))))

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

(def database-dialect ::my-mysql)
(register-dialect ::my-mysql)

(def database-quote-symbol \`)

(defmethod quote-name ::my-mysql [x]
  (str \` x \`))
