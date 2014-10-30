(defproject azql "0.2.1-SNAPSHOT"
  :description "DSL for SQL generation"
  :url "https://github.com/anjensan/azql"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/java.jdbc "0.3.6"]]
  :profiles
  {:h2
   {:dependencies [[com.h2database/h2 "1.4.182"]]
    :test-paths ["test-profile/h2"]},
   :mysql
   {:dependencies [[mysql/mysql-connector-java "5.1.33"]]
    :test-paths ["test-profile/mysql"]},
   :pgsql
   {:dependencies [[org.postgresql/postgresql "9.3-1102-jdbc41"]]
    :test-paths ["test-profile/pgsql"]},
   :dev
   [:h2]}
  :global-vars {*warn-on-reflection* true})
