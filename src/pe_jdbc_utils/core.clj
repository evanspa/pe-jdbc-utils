(ns pe-jdbc-utils.core
  "A set of helper functions for when working with relational databases."
  (:require [clojure.java.jdbc :as j]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.java.io :refer [resource]]
            [clojure.tools.logging :as log]))

(defmulti seq-next-val (fn [db-spec _] (:subprotocol db-spec)))

(defmethod seq-next-val "postgresql"
  [db-spec seq-name]
  (:nextval (j/query db-spec
                     [(format "select nextval('%s')" seq-name)]
                     :result-set-fn first)))

(defmulti uniq-constraint-violated? (fn [db-spec _] (:subprotocol db-spec)))

(defmethod uniq-constraint-violated? "postgresql"
  [db-spec ^org.postgresql.util.PSQLException e]
  (= "23505" (.getSQLState e)))

(defmulti uniq-constraint-violated (fn [db-spec _] (:subprotocol db-spec)))

(defmethod uniq-constraint-violated "postgresql"
  [db-spec ^org.postgresql.util.PSQLException e]
  (let [re #"\"([^\"]*)\""
        sem (.getServerErrorMessage e)
        msg (.getMessage sem)
        captured (re-find re msg)]
    (when (> (count captured) 1)
      (second captured))))

(defn incrementing-trigger-function-name
  [table column]
  (format "%s_%s_inc" table column));

(defmulti auto-incremented-trigger-function (fn [db-spec _ _] (:subprotocol db-spec)))

(defmethod auto-incremented-trigger-function "postgresql"
  [db-spec table column]
  (str (format "CREATE FUNCTION %s() RETURNS TRIGGER AS '"
               (incrementing-trigger-function-name table column))
       "BEGIN "
       (format "NEW.%s := OLD.%s + 1; " column column)
       "RETURN NEW; "
       "END; "
       "' LANGUAGE 'plpgsql'"))

(defmulti auto-incremented-trigger (fn [db-spec _ _ _] (:subprotocol db-spec)))

(defmethod auto-incremented-trigger "postgresql"
  [db-spec table column trigger-function]
  (str (format "CREATE TRIGGER %s_trigger BEFORE UPDATE ON %s "
               column
               table)
       (format "FOR EACH ROW EXECUTE PROCEDURE %s();"
               (incrementing-trigger-function-name table column))))

(defn with-try-catch-exec-as-query
  [db-spec stmt]
  (try
    (j/query db-spec stmt)
    (catch Exception e)))

(defmulti drop-database (fn [db-spec _] (:subprotocol db-spec)))

(defmethod drop-database "postgresql"
  [db-spec database-name]
  (with-try-catch-exec-as-query db-spec
    (format "drop database %s" database-name)))

(defmulti create-database (fn [db-spec _] (:subprotocol db-spec)))

(defmethod create-database "postgresql"
  [db-spec database-name]
  (with-try-catch-exec-as-query db-spec
    (format "create database %s" database-name)))
