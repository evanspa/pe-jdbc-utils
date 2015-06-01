(ns user
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer (pprint)]
            [clojure.repl :refer :all]
            [clojure.java.jdbc :as j]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.test :as test]
            [clojure.stacktrace :refer (e)]
            [pe-core-utils.core :as ucore]
            [pe-jdbc-utils.core :as jcore]
            [pe-jdbc-utils.test-utils :refer [db-spec-without-db
                                              db-spec
                                              db-spec-fn]]
            [clojure.tools.namespace.repl :refer (refresh refresh-all)]))

(def dev-db-name "dev")

(def db-spec-dev (db-spec-fn dev-db-name))

(defn refresh-dev-db
  []
  (jcore/drop-database db-spec-without-db dev-db-name)
  (jcore/create-database db-spec-without-db dev-db-name))
