(ns azql.test-database
  (:use [azql core dialect emit]))

(def database-connection
  {:classname "org.postgresql.Driver"
   :subprotocol "postgresql"
   :user "test"
   :password "test"
   :subname "//localhost/azql_test"
   :dialect ::my-pgsql})

(defn- do-commands
  [db & commands]
  (doseq [c commands]
    (execute! db (sql c))))

(defn create-database
  [db]
  (do-commands db

   "DROP TABLE IF EXISTS users"
   "CREATE TABLE users (
      id SERIAL PRIMARY KEY,
      name VARCHAR(50),
      dob DATE
    )"

   "DROP TABLE IF EXISTS posts"
   "CREATE TABLE posts (
      id SERIAL PRIMARY KEY,
      text VARCHAR(10000),
      userid INT
    )"

   "DROP TABLE IF EXISTS comments"
   "CREATE TABLE comments (
      id SERIAL PRIMARY KEY,
      text VARCHAR(500),
      userid INT,
      postid INT,
      parentid INT
    )"
   ))

(def database-dialect ::my-pgsql)
(register-dialect ::my-mysql)

(def database-quote-symbol \")
