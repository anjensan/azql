(ns azql.emit-test
  (:use clojure.test)
  (:use azql.emit))

(deftest test-sql-entities
  (is (every? sql? [(raw "") (raw "abc") (arg 1) (arg [1 2 3]) (qname :x) (batch-arg [1 2 3])]))
  (is (not-any? sql? [:keyword 'symbol 1 "string" \c '= [1 2] [(raw "sql")]])))

(deftest test-qnames

  (testing "qnames is sql"
    (is (sql? (qname :a))))

  (testing "parse qnames"
    (is (= [:a :b] (parse-qname :a.b)))
    (is (= [:a] (parse-qname :a)))
    (is (= [:a :b :c] (parse-qname :a.b.c))))

  (testing "emit qnames"
    (is (= "\"A\".\"B\"" (emit-qname :A.B)))
    (is (= "\"A\".\"b\".\"C\"") (emit-qname :A.b.C))
    (is (= "\"Abc\"" (emit-qname :Abc))))

  (testing "parse qualifier"
    (is (nil? (qualifier :a)))
    (is (nil? (qualifier "A")))
    (is (nil? (qualifier "A-b")))
    (is (nil? (qualifier nil)))
    (is (= :a (qualifier :a.b)))
    (is (= :a (qualifier :a.b.c)))
    (is (= :A (qualifier "A.B")))))

(deftest test-sql

  (testing "default implementations of SqlLike"
    (are [s z] (= s (:sql (as-sql z)))
         "\"keyword\"" :keyword
         "symbol" 'symbol
         " A~,~B " (symbol " A~,~B ")
         "\"a ~ b,c\"" (keyword "a ~ b,c")
         "?" 1
         "?" "string"
         "?" nil
         "? ?" [1 2]
         "?" (with-meta [1 2] {:batch true})
         "=" '=
         "X Y" ['X 'Y]))

  (testing "collect parameters"
    (are [a z] (= a (:args (as-sql z)))
         nil :keyword
         nil 'symbol
         [1] 1
         [1.12] 1.12
         [nil] nil
         [1 2 3] [1 [[2] 3]]
         ["str" 1] [(parenthesis [[:x '= "str"] AND [:y '<> 1]])]
         [[1 2 3] 4] [:x (with-meta [1 2 3] {:batch true}) :y 4]))

  (testing "test sql generation and formating"
    (are [sa z] (= (map->Sql sa) (sql* z))
         {:sql "SELECT * FROM \"Table\"" :args nil} [(raw "SELECT") :* (raw "FROM") :Table]
         {:sql "A  a B C D" :args nil} [[[(raw "A  a")] [(raw "B")]] [[[]]] [(raw "C")] (raw "D")]
         {:sql "A ( () )  ,  BC" :args nil} (raw "A ( () )  ,  BC")
         {:sql "A B" :args nil}  [NONE NONE (raw "A") NONE NONE NONE NONE (raw "B") NONE NONE]
         {:sql "A, B, C" :args nil} [(raw "A") NOSP COMMA (raw "B") NOSP COMMA (raw "C")]
         {:sql "AB" :args nil} [(raw "A") NOSP NOSP (raw "B")]
         {:sql "AB" :args nil} [(raw "A") NOSP NONE NOSP (raw "B")]
         {:sql "AB" :args nil} [(raw "A") NONE NOSP NONE (raw "B")]
         {:sql "A ? B ?" :args ["a" "b"]} [(raw "A") "a" (raw "B") "b"]
         {:sql "A ? B ?" :args [0 [1 2 3]]} ['A 0 'B (batch-arg [1 2 3])])))

(deftest test-helpers
  (is (= "((?) = (?))" (:sql (sql* (parenthesis [(parenthesis 1) '= (parenthesis 2)])))))
  (is (= "? + ?" (:sql (sql* (remove-parenthesis (parenthesis [1 '+ 2]))))))
  (is (= "?, ?, ?" (:sql (sql* (comma-list [1 2 3])))))
  (is (= "?, ?" (:sql (sql* (comma-list [(parenthesis 1) 2])))))
  (is (= 123 (remove-parenthesis (parenthesis 123))))
  (is (= 123 (remove-parenthesis 123))))

(deftest test-surrogate-aliases
  (is (surrogate-alias? (generate-surrogate-alias)))
  (is (not (surrogate-alias? :alias)))
  (is (not (surrogate-alias? :alias.subalias)))
  (is (not (surrogate-alias? "string")))
  (is (not= (generate-surrogate-alias) (generate-surrogate-alias)))
  (is (not= (as-alias [1]) (as-alias [1])))
  (is (= (as-alias-safe :ax) (as-alias-safe :ax)))
  (is (thrown? IllegalArgumentException (as-alias-safe [1])))
  (is (keyword? (as-alias [1]))))
