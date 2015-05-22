(ns pe-jdbc-utils.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [pe-jdbc-utils.core :as core]
            [pe-core-utils.core :as ucore]
            [clojure.java.io :refer [resource]]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :as log]
            [clj-time.core :as t]))
