(ns azql.integration-test
  (:use clojure.test)
  (:use [azql core emit])
  (:require [clojure.java.jdbc :as jdbc]))

(def h2-database-connection
  {:classname   "org.h2.Driver"
   :subprotocol "h2"
   :subname "mem://azql-test"})

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
          (where (in? :userid [1 3]))))))

(deftest test-aggregate-expressions
  (is
   (= 3
      (select
       (from :users)
       (fields {:x (count :*)})
       (fetch-single))))
  (is
   (= 1
      (select
       (fields [:parentid])
       (from :comments)
       (group [:parentid])
       (where (not-nil? :parentid))
       (having (> (count :id) 2))
       (fetch-single)))))

(deftest test-raw-queries
  (is (= 3 (count (fetch-all (sql '(select * from :users))))))
  (is (= 3 (fetch-single
            (sql ['select 'count LP '* RP 'from :users])))))

(deftest test-insert
  
  (testing "insert single record"
    (insert! :users {:name "New User" :dob #inst"1988-03-21T00:00"})
    (is (select (from :users) (where (= :name "New User")) (fetch-one)))
    (delete! (from :users) (where (= :name "New User")))
    (is (not (select (from :users) (where (= :name "New User")) (fetch-one)))))
  
  (testing "insert multiple records"
    (insert! :posts (values {:text "New Post 1"}) (values {:text "New Post 2" :userid 1}))
    (is (= 1 (select (from :posts) (fields [:userid]) (where (= :text "New Post 2")) (fetch-single))))
    (delete! (from :posts) (where (in? :text ["New Post 1" "New Post 2"])))
    (is (empty? (select (from :posts) (where (= :text "New Post 2")) (fetch-all))))))
    
   
      