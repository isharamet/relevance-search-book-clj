(ns relevant-search-book-clj.ch03
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
    (fn [acc movie]
      (let [[id {:keys [title overview tagline]}] movie]
        (conj
          acc
          {:index {:_index "tmdb", :_type "movie", :_id id}}
          {:title title, :overview overview, :tagline tagline})))
    []
    tmdb))

(defn reindex
  ([] (reindex nil))
  ([mappings]
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

(defn print-search-results
  [rs explain]
  (doseq [hit (get-in rs [:body :hits :hits])]
    (do
      (println (hit :_score) (get-in hit [:_source :title]))
      (if (true? explain)
        (clojure.pprint/pprint (hit :_explanation))))))

(defn search
  [q]
  (let [{:keys [query fields explain]
         :or {fields ["title^10" "overview"] explain false}} q]
   (spandex/request-async
     es-client
     {:url "/tmdb/movie/_search"
      :method :get
      :body {:query
             {:multi_match
              {:query query
               :fields fields}}
             :size 20
             :explain explain}
      :success (fn [rs] (print-search-results rs explain))
      :error   (fn [ex] (println ex))})))

(defn explain
  [q]
  (spandex/request-async
    es-client
    {:url "/tmdb/movie/_validate/query?explain"
     :method :get
     :body {:query
            {:multi_match
             {:query q
              :fields ["title^10" "overview"]}}}
     :success (fn [rs] (clojure.pprint/pprint (:body rs)))
     :error   (fn [ex] (println ex))}))

(defn analyze
  ([q] (analyze q nil))
  ([q field]
    (let [body {:analyzer :standard
                :text q}
          body (if (nil? field)
                 body
                 (assoc body :field field))]
      (spandex/request-async
        es-client
        {:url "/tmdb/_analyze"
         :method :get
         :body body
         :success (fn [rs] (clojure.pprint/pprint (:body rs)))
         :error   (fn [ex] (println ex))}))))

(defn get-mappings
  []
  (spandex/request-async
    es-client
    {:url "tmdb/_mappings"
     :method :get
     :success (fn [rs] (clojure.pprint/pprint (:body rs)))
     :error   (fn [ex] (println ex))}))

;; Indexing TMDB Movies

(reindex)

;; Searching for 'basketball with cartoon aliens'

(search {:query "basketball with cartoon aliens"})

;; Validating the query

(explain {:query "basketball with cartoon aliens"})

;; Analyzing the string

(analyze "Fire with Fire")

;; Reindexing with new settings

(reindex
  {:movie
   {:properties
    {:title
     {:type :text
      :analyzer :english}
     :overview
     {:type :text
      :analyzer :english}}}})

;; Inspecting the mappings

(get-mappings)

;; Reanalyzing the string

(analyze "Fire with Fire" :title)

;; Searching again

(search {:query "basketball with cartoon aliens"})

;; Decomposing Relevance Score With Lucene’s Explain

(search {:query "basketball with cartoon aliens"
         :explain true})

;; Fixing Space Jam vs Alien Ranking

(search {:query "basketball with cartoon aliens"
         :fields ["title^0.1" "overview"]})
