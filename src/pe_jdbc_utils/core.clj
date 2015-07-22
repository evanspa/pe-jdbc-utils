(ns pe-jdbc-utils.core
  "A set of helper functions for when working with relational databases."
  (:require [clojure.java.jdbc :as j]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.java.io :refer [resource]]
            [clojure.tools.logging :as log]
            [pe-core-utils.core :as ucore]))

(defmulti seq-next-val (fn [db-spec _] (:subprotocol db-spec)))

(defmethod seq-next-val "postgresql"
  [db-spec seq-name]
  (:nextval (j/query db-spec
                     [(format "select nextval('%s')" seq-name)]
                     :result-set-fn first)))

(defmulti uniq-constraint-violated? (fn [db-spec _] (:subprotocol db-spec)))

(defmethod uniq-constraint-violated? "postgresql"
  [db-spec e]
  (let [e (if (instance? java.sql.BatchUpdateException e)
            (.getNextException e)
            e)]
    (= "23505" (.getSQLState e))))

(defmulti uniq-constraint-violated (fn [db-spec _] (:subprotocol db-spec)))

(defmethod uniq-constraint-violated "postgresql"
  [db-spec e]
  (let [e (if (instance? java.sql.BatchUpdateException e)
            (.getNextException e)
            e)]
    (let [re #"\"([^\"]*)\""
          sem (.getServerErrorMessage e)
          msg (.getMessage sem)
          captured (re-find re msg)]
      (when (> (count captured) 1)
        (second captured)))))

(defn inc-trigger-fn-name
  [table column]
  (format "%s_%s_inc" table column));

(defmulti auto-inc-trigger-fn (fn [db-spec _ _] (:subprotocol db-spec)))

(defmethod auto-inc-trigger-fn "postgresql"
  [db-spec table column]
  (str (format "CREATE FUNCTION %s() RETURNS TRIGGER AS '"
               (inc-trigger-fn-name table column))
       "BEGIN "
       (format "NEW.%s := OLD.%s + 1; " column column)
       "RETURN NEW; "
       "END; "
       "' LANGUAGE 'plpgsql'"))

(defmulti auto-inc-trigger-single-non-nil-cond-fn (fn [db-spec _ _ _] (:subprotocol db-spec)))

(defmethod auto-inc-trigger-single-non-nil-cond-fn "postgresql"
  [db-spec table column cond-col]
  (str (format "CREATE FUNCTION %s() RETURNS TRIGGER AS '"
               (inc-trigger-fn-name table column))
       "BEGIN "
       (format "IF (OLD.%s IS NULL) AND (NEW.%s IS NOT NULL) THEN " cond-col cond-col)
       (format "NEW.%s := OLD.%s + 1; " column column)
       "END IF;"
       "RETURN NEW; "
       "END; "
       "' LANGUAGE 'plpgsql'"))

(defmulti auto-inc-trigger (fn [db-spec _ _ _] (:subprotocol db-spec)))

(defmethod auto-inc-trigger "postgresql"
  [db-spec table column trigger-fn]
  (str (format "CREATE TRIGGER %s_trigger BEFORE UPDATE ON %s "
               column
               table)
       (format "FOR EACH ROW EXECUTE PROCEDURE %s();"
               (inc-trigger-fn-name table column))))

(defn with-try-catch-exec-as-query
  [db-spec stmt]
  (try
    (j/query db-spec stmt)
    (catch Exception e)))

(defmulti drop-database (fn [db-spec _] (:subprotocol db-spec)))

(defmethod drop-database "postgresql"
  [db-spec database-name]
  (try
    (j/query db-spec (format "drop database %s" database-name))
    (catch org.postgresql.util.PSQLException e
      (let [sql-state (.getSQLState e)]
        (cond
          (= sql-state "3D000") false
          (= sql-state "02000") true
          :else (throw e))))))

(defmulti create-database (fn [db-spec _] (:subprotocol db-spec)))

(defmethod create-database "postgresql"
  [db-spec database-name]
  (try
    (j/query db-spec (format "create database %s" database-name))
    (catch org.postgresql.util.PSQLException e
      (let [sql-state (.getSQLState e)]
        (cond
          (= sql-state "42P04") false
          (= sql-state "02000") true
          :else (throw e))))))

(defn compute-deps-not-found-mask
  [entity any-issues-mask dep-checkers]
  (reduce (fn [mask [entity-load-fn dep-id-or-key dep-not-exist-mask]]
            (letfn [(do-dep-check [dep-id]
                      (let [loaded-entity-result (entity-load-fn dep-id)]
                        (if (nil? loaded-entity-result)
                          (bit-or mask dep-not-exist-mask any-issues-mask)
                          mask)))]
              (if (keyword? dep-id-or-key)
                (if (contains? entity dep-id-or-key)
                  (do-dep-check (get entity dep-id-or-key))
                  mask)
                (do-dep-check dep-id-or-key))))
          0
          dep-checkers))

(defn handle-non-unique-sqlexception
  [db-spec e any-issues-bit uniq-constraint-error-mask-pairs]
  (if (not (nil? uniq-constraint-error-mask-pairs))
    (if (uniq-constraint-violated? db-spec e)
      (let [ucv (uniq-constraint-violated db-spec e)]
        (let [mask (reduce (fn [mask [constraint-name already-exists-mask-bit]]
                             (if (= ucv constraint-name)
                               (bit-or mask already-exists-mask-bit any-issues-bit)
                               mask))
                           0
                           uniq-constraint-error-mask-pairs)]
          (if (not= mask 0)
            (throw (IllegalArgumentException. (str mask)))
            (throw e))))
      (throw e))
    (throw e)))

(defn save-if-unmodified-since
  [if-unmodified-since loaded-entity updated-at-entity-keyword do-save-fn]
  (if (not (nil? if-unmodified-since))
    (let [loaded-entity-updated-at (get loaded-entity updated-at-entity-keyword)]
      (if (and (not (nil? loaded-entity-updated-at))
               (t/after? loaded-entity-updated-at if-unmodified-since))
        (throw (ex-info nil {:type :precondition-failed :cause :unmodified-since-check-failed}))
        (do-save-fn)))
    (do-save-fn)))

(defn save-if-valid
  [validation-fn entity any-issues-bit do-save-fn]
  (if validation-fn
    (let [validation-mask (validation-fn entity)]
      (if (pos? (bit-and validation-mask any-issues-bit))
        (throw (IllegalArgumentException. (str validation-mask)))
        (do-save-fn)))
    (do-save-fn)))

(defn save-if-exists
  [db-spec entity-load-fn entity-id do-save-fn]
  (let [loaded-entity-result (entity-load-fn db-spec entity-id)]
    (if (nil? loaded-entity-result)
      (throw (ex-info nil {:cause :entity-not-found}))
      (do-save-fn (nth loaded-entity-result 1)))))

(defn save-if-deps-satisfied
  [entity any-issues-bit dep-checkers do-save-fn]
  (let [deps-not-found-mask (compute-deps-not-found-mask entity any-issues-bit dep-checkers)]
    (if (not= 0 deps-not-found-mask)
      (throw (IllegalArgumentException. (str deps-not-found-mask)))
      (do-save-fn))))

(defn save-rawmap
  [db-spec
   entity-id
   rawmap
   any-issues-bit
   entity-load-fn
   table-keyword
   updated-at-entity-keyword
   uniq-constraint-error-mask-pairs
   if-unmodified-since]
  (letfn [(do-update-entity []
            (let [updated-at (t/now)
                  updated-at-sql (c/to-timestamp updated-at)]
              (try
                (j/update! db-spec
                           table-keyword
                           (merge rawmap
                                  {:updated_at updated-at-sql}) ; fyi, updated_count gets updated via a trigger
                           ["id = ?" entity-id])
                ; so caller has latest entity with up-to-date updated_at and
                ; updated_count values
                (entity-load-fn db-spec entity-id)
                (catch java.sql.SQLException e
                  (handle-non-unique-sqlexception db-spec
                                                  e
                                                  any-issues-bit
                                                  uniq-constraint-error-mask-pairs)))))]
    ; whenever we save ANYTHING, we do if-exists and if-unmodified-since checks
    (save-if-exists db-spec
                    entity-load-fn
                    entity-id
                    (fn [loaded-entity]
                      (save-if-unmodified-since if-unmodified-since
                                                loaded-entity
                                                updated-at-entity-keyword
                                                do-update-entity)))))

(defn mark-entity-as-deleted
  ([db-spec
    entity-id
    entity-load-fn
    table-keyword
    updated-at-entity-keyword
    if-unmodified-since]
   (mark-entity-as-deleted db-spec
                           entity-id
                           entity-load-fn
                           table-keyword
                           updated-at-entity-keyword
                           if-unmodified-since
                           nil))
  ([db-spec
    entity-id
    entity-load-fn
    table-keyword
    updated-at-entity-keyword
    if-unmodified-since
    addl-map]
   (save-rawmap db-spec
                entity-id
                (merge {:deleted_at (c/to-timestamp (t/now))} addl-map)
                nil
                (fn [db-spec user-id] (entity-load-fn db-spec user-id false))
                table-keyword
                updated-at-entity-keyword
                nil
                if-unmodified-since)))

(defn entity-key-pairs->rawmap
  [entity entity-key-pairs]
  (reduce (fn [update-entity [entity-key column-key :as pairs]]
            (let [third-entry (if (> (count pairs) 2)
                                (nth pairs 2)
                                identity)]
              (if (fn? third-entry)
                (let [transform-fn third-entry]
                  (ucore/assoc-if-contains update-entity
                                           entity
                                           entity-key
                                           column-key
                                           transform-fn))
                (let [column-val third-entry]
                  (assoc update-entity column-key column-val)))))
          {}
          entity-key-pairs))

(defn save-entity
  [db-spec
   entity-id
   entity
   validation-fn
   any-issues-bit
   entity-load-fn
   table-keyword
   entity-key-pairs
   updated-at-entity-keyword
   uniq-constraint-error-mask-pairs
   dep-checkers
   if-unmodified-since]
  (save-if-valid validation-fn
                 entity
                 any-issues-bit
                 #(save-if-deps-satisfied entity
                                          any-issues-bit
                                          dep-checkers
                                          (fn []
                                            (save-rawmap db-spec
                                                         entity-id
                                                         (entity-key-pairs->rawmap entity entity-key-pairs)
                                                         any-issues-bit
                                                         entity-load-fn
                                                         table-keyword
                                                         updated-at-entity-keyword
                                                         uniq-constraint-error-mask-pairs
                                                         if-unmodified-since)))))

(defn save-new-entity
  [db-spec
   new-entity-id
   entity
   validation-fn
   any-issues-bit
   entity-load-fn
   table-keyword
   entity-key-pairs
   deps-insert-map
   created-at-entity-keyword
   updated-at-entity-keyword
   uniq-constraint-error-mask-pairs
   dep-checkers]
  (letfn [(do-insert-entity []
            (let [created-at (t/now)
                  created-at-sql (c/to-timestamp created-at)]
              (try
                (j/insert! db-spec
                           table-keyword
                           (merge (entity-key-pairs->rawmap entity entity-key-pairs)
                                  deps-insert-map
                                  {:id            new-entity-id
                                   :created_at    created-at-sql
                                   :updated_at    created-at-sql
                                   :updated_count 1}))
                ; so caller has latest entity with up-to-date updated_at and
                ; updated_count, etc values
                (entity-load-fn db-spec new-entity-id)
                (catch java.sql.SQLException e
                  (handle-non-unique-sqlexception db-spec
                                                  e
                                                  any-issues-bit
                                                  uniq-constraint-error-mask-pairs)))))]
    (save-if-valid validation-fn
                   entity
                   any-issues-bit
                   #(save-if-deps-satisfied entity
                                            any-issues-bit
                                            dep-checkers
                                            do-insert-entity))))

(defn active-only-where
  [active-only]
  (if active-only " and deleted_at is null" ""))

(defn load-entity-by-col
  [db-spec table col op col-val rs->entity-fn active-only]
  {:pre [(not (empty? table))
         (not (empty? col))
         (not (nil? col-val))
         (not (and (string? col-val)
                   (empty? col-val)))]}
  (let [rs (j/query db-spec
                    [(format "select * from %s where %s %s ?%s"
                             table
                             col
                             op
                             (active-only-where active-only)) col-val]
                    :result-set-fn first)]
    (when rs
      (rs->entity-fn rs))))

(defn load-entities-by-col
  [db-spec
   table
   col
   op
   col-val
   order-by-col
   order-by-direction
   rs->entity-fn
   active-only]
  {:pre [(not (empty? table))
         (not (empty? col))
         (not (nil? col-val))
         (not (and (string? col-val)
                   (empty? col-val)))]}
  (j/query db-spec
           [(format "select * from %s where %s %s ?%s order by %s %s"
                    table
                    col
                    op
                    (active-only-where active-only)
                    order-by-col
                    order-by-direction)
            col-val]
           :row-fn rs->entity-fn))
