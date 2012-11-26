(ns azql.expression-test
  (:use [azql expression emit dialect])
  (:use clojure.test))

;; custom dialect
(register-dialect ::dialect)
(deffunctions ::dialect fun sin funn)
(deffunctions ::dialect {funfun f} sin cos sqrt {ffun fun} my-fun)
(defmethod entity-naming-strategy ::dialect [] str)
(use-fixtures :once (fn [f] (binding [azql.dialect/*dialect* ::dialect] (f))))

(deftest test-render-expression
  (testing "test rendering of expressions"
    (is (= #azql.emit.Sql["((? > ?) AND (? < ?))" [1 2 3 4]]
           (sql* (render-expression ['and ['> 1 2] ['< 3 4]]))))
    (is (= #azql.emit.Sql["((? + ? + ?) * (? - ? - ?) * (- ?))" [1 2 3 4 5 6 7]]
           (sql* (render-expression ['* ['+ 1 2 3] ['- 4 5 6] ['- 7]]))))
    (is (= #azql.emit.Sql["(A = ?)" [1]]
           (sql* (render-expression ['= :A 1]))))
    (is (= #azql.emit.Sql["funn(?, ?)" [1 2]]
           (sql* (render-expression ['funn 1 2])))))

  (testing "test generic functions and operators"
    (are [a b] (= (apply ->Sql a) (sql* (render-expression b)))
         ["f()" nil] '(funfun)
         ["f(?)" [1]] '(funfun 1)
         ["my-fun(? + ?, ?, fun(), fun(?, ?))" [1 2 3 4 5]] '(my-fun (+ 1 2) 3 (fun) (ffun 4 5))
         ["sin(cos(?) + sqrt(?))", [1 2]] '(sin (+ (cos 1) (sqrt 2)))
         ["(? || ? || ?)", ["a" "b" "c"]] '(str "a" "b" "c"))))

(deftest test-prepare-macro-expr
  (testing "convert macro form to expr-tree"
    (are [a b] (= a (eval (prepare-macro-expression b)))
         123 123
         '[+ 1 2] '(+ 1 2)
         '[and [+ 1 2 3] :b] '(and (+ 1 2 3) :b)
         '[/ 10 2] '(/ 10 2)
         '[not= 1 2] '(not= 1 2)
         '[not= 1 2] '(<> 1 2)
         '[or [> 1 2] [< 1 2] [>= 1 2]] '(or (> 1 2) (< 1 2) (>= 1 2))
         '[not [nil? :x]] '(not (nil? :x))
         '[sin 1] '(sin 1)
         '[+ [sin 1] [cos :x]] '(+ (sin 1) (cos :x))
         '[= :x 1] {:x 1}
         '[and [= :a :b] [= 1 2]] (array-map :a :b, 1 2)
         '[or [= :x :y] [= 1 2]] '(or {:x :y} {1 2})
         '[= :x 3] '(= :x (clojure.core/+ 1 2)))))

(deftest test-null-aware-comparasions
  (testing "test null-aware comparasions"
    (are [a b] (= (str \( a \)) (:sql (sql* (render-expression b))))
         "x IS NULL" ['= nil :x]
         "y IS NULL" ['= :y nil]
         "x IS NOT NULL" ['not= nil :x]
         "y IS NOT NULL" ['not= :y nil]
         "? IS NULL" ['= nil nil]
         "? IS NOT NULL" ['not= nil nil])))

(deftest test-conj-boolean-expressions
  (testing "conj two expressions"
    (is (= ['and 1 2 3] (conj-expression ['and 1 2] 3)))
    (is (= :x (conj-expression nil :x)))
    (is (= :x (conj-expression () :x)))
    (is (= ['> 1 2] (conj-expression [] ['> 1 2])))
    (is (= ['and ['or :a :b] :c] (conj-expression ['and ['or :a :b]] :c)))))

(deftest test-like-operator
  (testing "test 'like' operator"
    (are [a b] (= a (:sql (sql* (render-expression b))))
         "(? LIKE ? ESCAPE '\\')" ['like? "a" "b"]
         "(x LIKE y ESCAPE '\\')" ['like? :x :y]
         "(x LIKE ? ESCAPE '\\')" ['starts? :x "abc"]))
  (testing "test 'starts?' alias"
    (is
     (= ["x" "abc%"] (:args (sql* (render-expression ['starts? "x" "abc"])))))))

(deftest test-dialects-specific-op
  (testing "dialect-specific operation"
     (defoperator myfun [] :myfun-default)
     (defoperator myfun ::custom-dialect [] :myfun-dialect)
     (deffunctions ::custom-dialect [f1] f2 {f3 Fx3})
     (is (= :myfun-default (render-operator 'myfun)))
     (is (= :myfun-dialect (binding [*dialect* ::custom-dialect]
                             (render-operator 'myfun))))
     (are [a b] (= a (:sql (sql* (binding [*dialect* ::custom-dialect]
                                   (render-expression b)))))
          "f1()" ['f1]
          "f2(?)" ['f2 1]
          "Fx3()" ['f3])))


(deftest test-case
  (testing "test 'case' operator"
    (are [a b] (= a (:sql (sql* (render-expression b))))
         "CASE x WHEN a THEN b END" ['case :x :a :b]
         "CASE x WHEN a THEN b ELSE c END" ['case :x :a :b :c]
         "CASE ? WHEN ? THEN ? WHEN ? THEN ? ELSE ? END" ['case nil nil nil nil nil nil]))
  (testing "test 'cond' operator"
    (are [a b] (= a (:sql (sql* (render-expression b))))
         "CASE WHEN a THEN b END" ['cond :a :b]
         "CASE WHEN x THEN a WHEN b THEN c END" ['cond :x :a :b :c]
         "CASE WHEN x THEN a ELSE b END" ['cond :x :a :b]
         "CASE WHEN (a = b) THEN c ELSE d END" ['cond ['= :a :b] :c :d]
         "CASE WHEN (a IS NULL) THEN c ELSE d END" ['cond ['= :a nil] :c :d]
         "CASE WHEN ? THEN ? WHEN ? THEN ? ELSE ? END" ['cond nil nil nil nil nil])))
