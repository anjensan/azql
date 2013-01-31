(defproject azql "0.1.0"
  :description "DSL for SQL generation"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/java.jdbc "0.2.3"]]
  :profiles
  {:h2
   {:dependencies [[com.h2database/h2 "1.3.170"]]
    :test-paths ["test-profile/h2"]},
   :mysql
   {:dependencies [[mysql/mysql-connector-java  "5.1.17"]]
    :test-paths ["test-profile/mysql"]},
   :dev [:h2]}
  :warn-on-reflection true)
