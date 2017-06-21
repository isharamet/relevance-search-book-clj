(ns relevant-search-book-clj.ch06
  (:require [relevant-search-book-clj.core :refer :all]
            [qbits.spandex :as spandex]))

(spandex/request
  es-client
  {:url "/albinoelephant/docs/1"
   :method :put
   :body {:title :albino
          :body :elephant}})

(spandex/request
  es-client
  {:url "/albinoelephant/docs/2"
   :method :put
   :body {:title :elephant
          :body :elephant}})

(spandex/request-async
  es-client
  {:url "/albinoelephant/docs/_search"
   :method :get
   :body
   {:query
    {:multi_match
     {:query "albino elephant"
      :fields ["title" "body"]
      :type :most_fields}}}
   :success (fn [rs] (clojure.pprint/pprint (:body rs)))
   :error   (fn [ex] (println ex))})
