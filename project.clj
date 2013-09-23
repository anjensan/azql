(defproject azql "0.2.0-SNAPSHOT"
  :description "DSL for SQL generation"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/java.jdbc "0.3.0-alpha5"]]
  :profiles
  {:h2
   {:dependencies [[com.h2database/h2 "1.3.173"]]
    :test-paths ["test-profile/h2"]},
   :mysql
   {:dependencies [[mysql/mysql-connector-java "5.1.26"]]
    :test-paths ["test-profile/mysql"]},
   :pgsql
   {:dependencies [[postgresql "9.1-901-1.jdbc4"]]
    :test-paths ["test-profile/pgsql"]},
   :dev
   [:h2]}
  :global-vars {*warn-on-reflection* true})
