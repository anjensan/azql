(ns azql.integration-test
  (:use clojure.test)
  (:use [azql core emit])
  (:require [clojure.java.jdbc :as jdbc])
  (:use azql.test-database))

(defn sql-date [^java.util.Date x]
  (java.sql.Date. (.getTime x)))

(def ^:dynamic ^:private db)

(defn populate-database
  [db]
  ;; 3 users, each user has 3 comments & 2 or 0 posts

  (insert!
   db :users
   (values
    (map (partial zipmap [:id :name :dob])
         [[1 "Artyom" (sql-date #inst"1980-01-21")]
          [2 "Anton" (sql-date #inst"1981-02-22")]
          [3 "Arturas" (sql-date #inst"1982-03-23")]])))

  (insert!
   db :posts
   (values
    (map (partial zipmap [:id :text :userid])
         [[1 "first post" 1]
          [2 "second post" 1]
          [3 "the post" 2]
          [4 "repost" 2]])))

  (insert!
   db :comments
   (values
    (map (partial zipmap [:id :text :userid :postid :parentid])
         [[1 "comment1" 1 1 nil]
          [2 "comment1" 1 2 1]
          [3 "comment1" 1 3 2]
          [4 "comment1" 2 1 nil]
          [5 "comment1" 2 2 nil]
          [6 "comment1" 2 1 nil]
          [7 "comment1" 3 1 nil]
          [8 "comment1" 3 2 1]
          [9 "comment9" 3 3 1]]))))

(defn create-testdb-fixture
  [f]
  (with-connection [conn database-connection]
    (binding [db conn]
      (create-database db)
      (populate-database db)
      (f))))

(defn transaction-fixture
  [f]
  (try
    (transaction db
      (f)
      (throw (ex-info "rollback" {::x 1})))
    (catch Exception e
      (when-not (::x (ex-data e)) (throw e)))))

(use-fixtures :once create-testdb-fixture)
(use-fixtures :each transaction-fixture)

;; tests

(deftest test-simple-selects

  (testing "select all entities from table (test count)"
           (are [c tbl] (= c (count (fetch-all db (select (from tbl)))))
         3 :users, 4 :posts, 9 :comments))

  (testing
    "select all entities (order by name)"
    (is (=
          [{:id 2 :name "Anton"}, {:id 3 :name "Arturas"}, {:id 1 :name "Artyom"}]
          (fetch-all
           db
           (select
            (fields [:id :name])
            (from :users)
            (order :name)
            (offset 0))))))

  (testing
    "lazy select all entities and collect ids"
    (is (=
          [1 2 3]
          (with-fetch db [v (select (from :users))] (reduce #(conj %1 (:id %2)) [] v)))))

  (testing
    "get user by id"
    (is (=
          {:id 2 :n "Anton"}
          (fetch-one db
            (select (from :users) (fields {:id :id :n :name}) (where (= :id 2)))))))

  (testing
    "get user name by id"
    (is (=
          {:name "Arturas"}
          (fetch-one db (select (fields [:name]) (from :users) (where (= :id 3)))))))

  (testing
    "get only user name"
    (is (=
          "Arturas"
          (fetch-single db (select (from :users) (where (= :id 3)) (fields [:name]))))))

  (testing
    "get posts without parent"
    (is (= 5 (count (fetch-all db (select (from :comments) (where (= :parentid nil)))))))))


(deftest test-operators

  (testing
    "string concatenation"
    (is (=
          "2-x-Anton"
          (fetch-single db
           (select
            (from :users)
            (fields [(str :id "-x-" :name)])
            (where (= :id 2))))))))

(deftest test-like-operator
  (testing
    "test like operator"
    (is (=
          "Arturas"
          (fetch-single db (select [:name] (from :users) (where (like? :name "%turas"))))))
    (is (=
          "Arturas"
          (fetch-single db (select [:name] (from :users) (where (starts? :name "Artur"))))))
    (is
      (nil?
        (fetch-single db (select (from :users) (where (starts? :name "Artur%"))))))))

(deftest test-multi-value-expressions
  (testing
    "operator 'in'"
    (are [cnt s]
         (= cnt (count (fetch-all db s)))
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
  (is (= 3 (fetch-single db (select (from :users) (fields [(count :*)])))))
  (is (= 1 (fetch-single
            db
            (select (fields [:parentid]) (from :comments)
                    (group [:parentid]) (where (not-nil? :parentid))
                    (having (> (count :id) 2)))))))

(deftest test-raw-queries
  (is (= 3 (count (fetch-all db (sql db 'select '* 'from :users)))))
  (is (= 3 (fetch-single db
             (sql db 'select 'count NOSP LEFT_PAREN NOSP '* NOSP RIGHT_PAREN 'from :users)))))

(deftest test-insert

  (testing
    "insert single record"
    (insert! db :users {:id 10 :name "New User" :dob (sql-date #inst"1988-03-21T00:00")})
    (fetch-all db (select (from :users)))
    (is (fetch-one db (select (from :users) (where (= :name "New User")))))
    (delete! db :users (where (= :name "New User")))
    (is (not (fetch-one db (select (from :users) (where (= :name "New User")))))))

  (testing
    "insert multiple records"
    (insert! db
     :posts
     (values {:id 11 :text "New Post 1"})
     (values {:id 12 :text "New Post 2" :userid 1}))
    (is (= 1 (fetch-single db (select (from :posts) (fields [:userid]) (where (= :text "New Post 2"))))))
    (delete! db :posts (where (in? :text ["New Post 1" "New Post 2"])))
    (is (empty? (fetch-all db (select (from :posts) (where (= :text "New Post 2"))))))))

(deftest test-update

  (testing
    "update one record"
    (update! db :users (setf :name "XXX") (where (= :name "Anton")))
    (is (= 1 (count (fetch-all db (select (from :users) (where (= :name "XXX"))))))))

  (testing
    "update all records"
    (update! db :users (setf :name "XXX"))
    (is (= 3 (count (fetch-all db (select (from :users) (where (= :name "XXX")))))))))

(deftest test-transaction
  (testing
      "transaction ok")
  (testing
      "nested transactions")
  (testing
      "transaction level")
  (testing
      "transaction without connection")
  (testing
      "transaction rollback"))
