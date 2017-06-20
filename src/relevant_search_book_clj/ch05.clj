(ns relevant-search-book-clj.ch05
  (:require [relevant-search-book-clj.core :refer :all]
            [qbits.spandex :as spandex]))


(def index-data
  (reduce
    (fn [acc movie]
      (let [[id data] movie]
        (conj
          acc
          {:index {:_index "tmdb", :_type "movie", :_id id}}
          data)))
    []
    tmdb))

(defn get-movie-by-id
  [id]
  (spandex/request-async
    es-client
    {:url (str "/tmdb/movie/" id)
     :method :get
     :success (fn [rs] (clojure.pprint/pprint (:body rs)))
     :error   (fn [ex] (println ex))}))
;; ---

(reindex index-data)

(def space-jam-id 2300)

(get-movie-by-id space-jam-id)
