(ns azql.dialect
  (:use [azql util])
  (:require [clojure.java.jdbc :as jdbc]))

(def ^:const default-dialect ::sql92)
(def dialects-hierarchy (make-hierarchy))

(def ^{:doc "Current dialect." :dynamic true} *dialect* nil)

(defn parse-jdbc-protocol
  "Parses JDBC connection URL and returns protocol."
  [url]
  (keyword (get (re-find #"jdbc:([^:]+):.*" url) 1)))

(defmulti guess-dialect
  "Recognizes database dialect by connection URL.
   Method should return keyword."
  (fn [^java.sql.DatabaseMetaData metadata]
    (parse-jdbc-protocol (.getURL metadata))))

;; use ::sql92 for all unknown databases
(defmethod guess-dialect :default [_] default-dialect)

(defn current-jdbc-connection-dialect
  "Guess dialect for current JDBC connection."
  []
  (when-let [^java.sql.Connection conn (jdbc/find-connection)]
    (guess-dialect (.getMetaData conn))))

(defn current-dialect
  "Returns current SQL dialect.
   Accepts any number of arguments and ignores them."
  [& _]
  (or
    *dialect*
    (:azql/dialect @#'jdbc/*db*)
    (current-jdbc-connection-dialect)
    default-dialect))

(defn register-dialect
  "Registers new dialect (adds it to hierarchy)."
  ([dialect parent]
    (alter-var-root #'dialects-hierarchy derive dialect parent))
  ([dialect]
    (register-dialect dialect default-dialect)))

(defmacro defndialect
  "Defines multimethod, dispatched by current dialect.
   Note: multimethod uses separate hierarchy `dialects-hierarchy`."
  [name & args-and-body]
  (let [[doc & body]
        (if (string? (first args-and-body))
          args-and-body
          (cons nil args-and-body))
        name
        (if doc
          (vary-meta name assoc :doc doc)
          name)]
    `(do
       (defmulti ~name current-dialect :hierarchy #'dialects-hierarchy)
       (defmethod ~name default-dialect ~@body))))

(defmacro with-recognized-dialect
  "Recognizes dialect of current connection and adds it to jdbc/*db*."
  [& body]
  `(with-recognized-dialect* (fn [] ~@body)))

(defn with-recognized-dialect*
  "Recognizes dialect of current connection and adds it to jdbc/*db*."
  [f]
  (if (find @#'jdbc/*db* :azql/dialect)
    (f)
    (if-let [d (or *dialect* (current-jdbc-connection-dialect))]
      (with-bindings* {#'jdbc/*db* (assoc @#'jdbc/*db* :azql/dialect d)} f)
      (f))))
