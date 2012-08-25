(ns azql.connection
  (:use azql.util)
  (:require [clojure.java.jdbc :as jdbc]))

(def ^:dynamic ^{:doc "Global connection."} ^java.sql.Connection global-connection nil)

(defn with-global-connection*
  "Executes function in a scope of the global connection."
  [f]
  (if (jdbc/find-connection)
    (f)
    (if-let [c global-connection]
      ; reuse opened connection
      (with-bindings*
        ; copied from `clojure.java.jdbc/with-connection`
        {#'jdbc/*db*
         (assoc @#'jdbc/*db*
           :connection c
           :level 0
           :rollback (atom false))}
        f)
      (illegal-state "No global connection available."))))
  
(defmacro with-global-connection
  "Executes code in a scope of the global connection."
  [& bs]
  `(with-global-connection* (fn [] ~@bs)))

(defn open-global-connection
  "Opens global connection."
  [db-spec]
  (alter-var-root
   #'global-connection
   (fn [^java.sql.Connection c]
     (when c (.close c))
     (when db-spec (#'jdbc/get-connection db-spec)))))

(defn close-global-connection
  "Closes global connection."
  []
  (open-global-connection nil))