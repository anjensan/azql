(ns azql.expression-test
  (:use [azql expression emit])
  (:use clojure.test))

(deftest test-render-expr
  (testing "test rendering of expressions"
    (is (= #azql.emit.Sql["((? > ?) AND (? < ?))" [1 2 3 4]]
           (sql (render-expression ['and ['> 1 2] ['< 3 4]]))))
    (is (= #azql.emit.Sql["((? + ? + ?) * (? - ? - ?) * (- ?))" [1 2 3 4 5 6 7]]
           (sql (render-expression ['* ['+ 1 2 3] ['- 4 5 6] ['- 7]]))))
    (is (= #azql.emit.Sql["(\"A\" = ?)" [1]]
           (sql (render-expression ['= :A 1]))))
    (is (= #azql.emit.Sql["(funn(?, ?))" [1 2]]
           (sql (render-expression ['funn 1 2]))))))

(deftest test-prepare-macro-expr
  (testing "convert macro form to expr-tree"
    (are [a b] (= a (eval (prepare-macro-expression b)))
         123 123
         '[+ 1 2] '(+ 1 2)
         '[and [+ 1 2 3] :b] '(and (+ 1 2 3) :b)
         '[/ 10 2] '(/ 10 2)
         '[<> 1 2] '(not= 1 2)
         '[or [> 1 2] [< 1 2] [>= 1 2]] '(or (> 1 2) (< 1 2) (>= 1 2))
         '[not [nil? :x]] '(not (nil? :x))
         '[sin 1] '(sin 1)
         '[+ [sin 1] [cos :x]] '(+ (sin 1) (cos :x)))))

(deftest test-null-aware-comparasions
  (testing "test null-aware comparasions"
    (are [a b] (= (str \( a \)) (:sql (sql (render-expression b))))
         "\"x\" IS NULL" ['= nil :x]
         "\"y\" IS NULL" ['= :y nil]
         "\"x\" IS NOT NULL" ['<> nil :x]
         "\"y\" IS NOT NULL" ['<> :y nil]
         "? IS NULL" ['= nil nil]
         "? IS NOT NULL" ['<> nil nil])))

(deftest test-conj-boolean-expressions
  (testing "conj two expressions"
    (is (= ['and 1 2 3] (conj-expression ['and 1 2] 3)))
    (is (= :x (conj-expression nil :x)))
    (is (= :x (conj-expression () :x)))
    (is (= ['> 1 2] (conj-expression [] ['> 1 2])))
    (is (= ['and ['or :a :b] :c] (conj-expression ['and ['or :a :b]] :c)))))
