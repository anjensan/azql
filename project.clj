(defproject azql "0.1.0"
  :description "DSL for SQL generation"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/java.jdbc "0.2.3"]]
  :profiles
  {:dev
   {:dependencies [[com.h2database/h2 "1.3.170"]]}}
  :warn-on-reflection true)
