(ns azql.integration-test
  (:use clojure.test)
  (:use azql.core)
  (:require [clojure.java.jdbc :as jdbc]))

(def h2-database-connection
  {:classname   "org.h2.Driver"
   :subprotocol "h2"
   :subname "mem://azql-test"})

(defn create-database
  []
  (jdbc/create-table
   :users
   [:id "INT" "PRIMARY KEY"]
   [:name "VARCHAR(50)"]
   [:dob "DATE"])
  (jdbc/create-table
   :posts
   [:id "INT" "PRIMARY KEY"]
   [:text "VARCHAR(10000)"]
   [:userid "INT"])
  (jdbc/create-table
   :comments
   [:id "INT" "PRIMARY KEY"]
   [:text "VARCHAR(500)"]
   [:userid "INT"]
   [:postid "INT"]
   [:parentid "INT"]))

(defn populate-database
  []
  ;; 3 users, each user has 3 comments & 2 or 0 posts
  (jdbc/insert-rows
   :users
   [1 "Artyom" #inst"1980-01-21"]
   [2 "Anton" #inst"1981-02-22"]
   [3 "Arturas" #inst "1982-03-23"])
  (jdbc/insert-rows
   :posts
   [1 "first post" 1]
   [2 "second post" 1]
   [3 "the post" 2]
   [4 "repost" 2])
  (jdbc/insert-rows
   :comments
   [1 "comment1" 1 1 nil]
   [2 "comment1" 1 2 1]
   [3 "comment1" 1 3 2]
   [4 "comment1" 2 1 nil]
   [5 "comment1" 2 2 nil]
   [6 "comment1" 2 1 nil]
   [7 "comment1" 3 1 nil]
   [8 "comment1" 3 2 1]
   [9 "comment9" 3 3 1]))

(defn create-testdb-fixture
  [f]
  (jdbc/with-connection h2-database-connection
    (jdbc/with-quoted-identifiers \"
      (create-database)
      (populate-database))
    (f)))

(defn transaction-fixture
  [f]
  (jdbc/transaction
   (jdbc/set-rollback-only)
   (f)))

(use-fixtures :once create-testdb-fixture)
(use-fixtures :each transaction-fixture)

;; tests

(deftest test-simple-selects
  
  (testing "select all entities from table (test count)"
    (are [c tbl] (= c (count (select (from tbl) (fetch-all))))
         3 :users
         4 :posts
         9 :comments))
  
  (testing "select all entities (order by name)"
    (is (=
         [{:id 2 :name "Anton"}
          {:id 3 :name "Arturas"}
          {:id 1 :name "Artyom"}]
         (select
          (fields [:id :name])
          (from :users)
          (order :name)
          (offset 0)
          (fetch-all)))))

  (testing "lazy select all entities and collect ids"
    (is (=
         [1 2 3]
         (with-fetch [v (select (from :users))]
           (reduce #(conj %1 (:id %2)) [] v)))))

  (testing "get user by id"
    (is (=
         {:id 2 :n "Anton"}
         (select
          (from :users)
          (fields {:id :id :n :name})
          (where (= :id 2))
          (fetch-one)))))
  
  (testing "get user name by id"
    (is (=
         {:name "Arturas"}
         (select
          (fields [:name])
          (from :users)
          (where (= :id 3))
          (fetch-one)))))

  (testing "get only user name"
    (is (=
         "Arturas"
         (select
          (from :users)
          (where (= :id 3))
          (fields [:name])
          (fetch-single)))))

  (testing "get posts without parent"
    (is (=
         5
         (select
          (from :comments)
          (where (= :parentid nil))
          (fetch-all)
          (count))))))
           
(deftest test-multi-value-expressions
  (testing "test 'in' operator"
    (are [cnt s]
         (= cnt (count (fetch-all s)))
         9
         (select
          (from :comments)
          (where (in? :userid [1 2 3])))
         3
         (select
          (from :comments)
          (where (not-in? :userid [1 3])))
         0
         (select
          (from :comments)
          (where (in? :userid [])))
         9
         (select
          (from :comments)
          (where (not-in? :userid [])))
         6
         (select
          (from :comments)
          (where (contains? :userid [1 3]))))))
