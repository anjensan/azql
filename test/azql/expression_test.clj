(ns azql.expression-test
  (:use [azql expression emit])
  (:use clojure.test))

(deftest test-render-expr
  (testing "test rendering of expressions"
    (is (= #azql.emit.Sql["( ( ? > ? ) AND ( ? < ? ) )" [1 2 3 4]]
           (sql (render-expression [:and [:> 1 2] [:< 3 4]]))))
    (is (= #azql.emit.Sql["( ( ? + ? + ? ) * ( ? - ? - ? ) * ( - ? ) )" [1 2 3 4 5 6 7]]
           (sql (render-expression [:* [:+ 1 2 3] [:- 4 5 6] [:- 7]]))))
    (is (= #azql.emit.Sql["( \"A\" = ? )" [1]]
           (sql (render-expression [:= :A 1]))))
    ))

(deftest test-expresison-symbol
  (testing "expr symbol test"
    (is (expression-symbol? '>))
    (is (expression-symbol? '<>))
    (is (not (expression-symbol? 'conj)))
    (is (not (expression-symbol? ">")))
    (is (not (expression-symbol? 1)))
    (is (not (expression-symbol? nil)))
    (is (not (expression-symbol? :=)))))

(deftest test-prepare-macro-expr
  (testing "convert macro form to expr-tree"
    (are [a b] (= a (prepare-macro-expression b))
         123 123
         [:+ 1 2] '(+ 1 2)
         [:and [:+ 1 2 3] :b] '(and (+ 1 2 3) :b)
         [:div 10 2] '(/ 10 2)
         [:<> 1 2] '(not= 1 2)
         '[:+ x :b 3] '(+ x :b 3)
         [:or [:> 1 2] [:< 1 2] [:>= 1 2]] '(or (> 1 2) (< 1 2) (>= 1 2))
         '[:not [:nil? x]] '(not (nil? x))
         ))
  (testing "conj two expressions"
    (is (= [:and 1 2 3] (conj-expression [:and 1 2] 3)))
    (is (= :x (conj-expression nil :x)))
    (is (= :x (conj-expression () :x)))
    (is (= [:> 1 2] (conj-expression [] [:> 1 2])))
    (is (= [:and [:or :a :b] :c] (conj-expression [:and [:or :a :b]] :c))))
  )

        