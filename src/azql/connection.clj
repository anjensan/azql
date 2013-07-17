(ns azql.connection
  (:use [azql util])
  (:require [clojure.java.jdbc :as jdbc]))

(def ^:dynamic ^:private ^java.sql.Connection *db* nil)

(defn get-current-db
  "Returns current database. For internal use only!"
  []
  *db*)

(defmacro with-connection
  [db & body]
  `(let [db# ~db
         conn# (jdbc/db-find-connection db#)
         bfn# (fn [] ~@body)]
     (if (nil? conn#)
       (with-open [^java.sql.Connection conn# (jdbc/get-connection db#)]
         (let [new-db# (jdbc/add-connection db# conn#)]
           (with-bindings* {#'*db* new-db#} bfn#)))
       (with-bindings* {#'*db* db#} bfn#))))
