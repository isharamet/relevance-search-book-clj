(ns relevant-search-book-clj.core
  (:require [cheshire.core :as cheshire]
            [qbits.spandex :as spandex]
            [clojure.core.async :as async]))

(def tmdb
  (with-open [rdr (clojure.java.io/reader "resources/tmdb.json")]
    (cheshire/parse-stream rdr true)))

(def es-client
  (spandex/client {:hosts ["http://localhost:9200"]}))

(defn reindex
  ([index-data] (reindex index-data nil))
  ([index-data mappings]
   (let [settings {:settings {:number_of_shards 1}}
         body (if (nil? mappings)
                settings
                (assoc settings :mappings mappings))]
     (do
       (try
         (spandex/request
           es-client
           {:url "/tmdb"
            :method :delete})
         (catch Exception e
           (println "Unable to delete 'tmdb' index")))
       (spandex/request
         es-client
         {:url "/tmdb"
          :method :put
          :body body})
       (let [{:keys [input-ch output-ch]}
             (spandex/bulk-chan
               es-client
               {:flush-threshold 100
                :flush-interval 5000
                :max-concurrent-requests 3})]
         (async/put! input-ch index-data))
       (future (loop [] (async/<!! (:output-ch es-client))))))))
