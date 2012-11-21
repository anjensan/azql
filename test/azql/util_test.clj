(ns azql.util-test
  (:use clojure.test)
  (:use azql.util))

(deftest test-map-vals
  (is (= {:x 1 :y 2} (map-vals inc {:x 0 :y 1})))
  (is (instance? (class (sorted-map)) (map-vals inc (sorted-map :x 1 :y 2))))
  (is (= {} (map-vals inc {})))
  (is (= {} (map-vals inc nil))))

(deftest test-emit-threaded-expression
  (is (= `(-> (a) (b) (c)) (emit-threaded-expression `a `((b) (c)))))
  (is (= `(-> (a x) (b) (c)) (emit-threaded-expression `a `(x (b) (c)))))
  (is (= `(-> (a x y) (b) c)) (emit-threaded-expression `a `(x y (b) c)))
  (is (= `(-> (a [x]) (b)) (emit-threaded-expression `a `([x] (b))))))

(deftest test-checks
  (is (thrown? IllegalArgumentException (check-argument false "message")))
  (is (thrown? IllegalStateException (check-state false "message")))
  (is (nil? (check-argument true "message")))
  (is (nil? (check-state true "message"))))
