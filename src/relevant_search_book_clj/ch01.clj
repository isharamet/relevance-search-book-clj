(ns relevant-search-book-clj.ch01
  (:require [cheshire.core :as cheshire]
            [qbits.spandex :as spandex]
            [qbits.spandex.utils :as spandex-utils]
            [clojure.core.async :as async]))

(def tmdb
  (with-open [rdr (clojure.java.io/reader "resources/tmdb.json")]
    (cheshire/parse-stream rdr true)))

(def es-client
  (spandex/client {:hosts ["http://localhost:9200"]}))

(def index-data
  (reduce
    (fn [acc m]
      (let [[id {:keys [title overview tagline]}] m]
        (conj acc
              {:index {:_index "tmdb", :_type "movie", :_id id}}
              {:title title, :overview overview, :tagline tagline})))
    []
    tmdb))

(defn reindex
  []
  (do
    (try
      (spandex/request es-client
                       {:url "/tmdb"
                        :method :delete})
      (catch Exception e
        (println "Unable to delete 'tmdb' index")))
    (spandex/request es-client
                     {:url "/tmdb"
                      :method :put
                      :body {:settings {:number_of_shardssss 1}}})
    (let [{:keys [input-ch output-ch]} (spandex/bulk-chan es-client
                                                          {:flush-threshold 100
                                                           :flush-interval 5000
                                                           :max-concurrent-requests 3})]
      (async/put! input-ch index-data))
    (future (loop [] (async/<!! (:output-ch es-client))))))

(reindex)

(defn query
  [q]
  (spandex/request-async es-client {:url "/tmdb/movie/_search"
                                    :method :get
                                    :body
                                      {:query
                                        {:multi_match
                                          {:query q
                                           :fields ["title^10" "overview"]}}}
                                    :success (fn [rs] (println rs))
                                    :error (fn [ex] (println ex))}))

(query "titanic")