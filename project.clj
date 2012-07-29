(defproject azql "0.1.0-SNAPSHOT"
  :description "DSL for SQL generation"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/java.jdbc "0.2.3"]]
  :plugins [[lein-swank "1.4.4"]]
  :profiles
  {:dev {:dependencies [[com.h2database/h2 "1.3.168"]]}}
  :warn-on-reflection true)
