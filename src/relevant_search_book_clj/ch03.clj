(ns relevant-search-book-clj.ch03
  (:require [relevant-search-book-clj.core :refer :all]
            [qbits.spandex :as spandex]))

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

(reindex index-data)

;; Searching for 'basketball with cartoon aliens'

(search {:query "basketball with cartoon aliens"})

;; Validating the query

(explain "basketball with cartoon aliens")

;; Analyzing the string

(analyze "Fire with Fire")

;; Reindexing with new settings

(reindex
  index-data
  {:settings
   {:number_of_shards 1}
   :mappings
    {:movie
     {:properties
      {:title
       {:type :text
        :analyzer :english}
       :overview
        {:type :text
         :analyzer :english}}}}})

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
