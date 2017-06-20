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

(defn search
  [q]
  (spandex/request-async
    es-client
    {:url "/tmdb/movie/_search"
     :method :get
     :body q
     :success (fn [rs] (print-search-results rs false))
     :error   (fn [ex] (println ex))}))

;; ---

(reindex
  index-data
  {:settings
   {:number_of_shards 1
    :index
     {:analysis
      {:analyzer
       {:default
        {:type :english}}}}}})

(def space-jam-id 2300)

(get-movie-by-id space-jam-id)

;; ---

(search
  {:query
   {:multi_match
    {:query "patrick stewart"
     :fields ["title" "overview" "cast.name" "directors.name"]
     :type :best_fields}}
   :size 10})

;; ---

(search
  {:query
   {:multi_match
    {:query "patrick stewart"
     :fields ["title" "overview" "cast.name" "directors.name^0.1"]
     :type :best_fields}}
   :size 10})

;; ---

(reindex
  index-data
  {:settings
   {:number_of_shards 1
    :index
     {:analysis
      {:analyzer
       {:default
         {:type :english}
        :english_bigrams
         {:type :custom
          :tokenizer :standard
          :filter
          [:standard
           :lowercase
           :porter_stem
           :bigram_filter]}}
       :filter
        {:bigram_filter
         {:type :shingle
          :max_shingle_size 2
          :min_shingle_size 2
          :output_unigrams "false"}}}}}
   :mappings
    {:movie
     {:properties
      {:cast
       {:properties
        {:name
         {:type :text
          :analyzer :english
          :fields
          {:bigramed
           {:type :text
            :analyzer :english_bigrams}}}}}
       :directors
        {:properties
         {:name
          {:type :text
           :analyzer :english
           :fields
           {:bigramed
            {:type :text
             :analyzer :english_bigrams}}}}}}}}})

(search
  {:query
   {:multi_match
    {:query "patrick stewart"
     :fields ["title" "overview" "cast.name.bigramed" "directors.name.bigramed"]
     :type :best_fields}}
   :size 10})

;; ---

(search
  {:query
   {:multi_match
    {:query "star trek patrick stewart"
     :fields ["title" "overview"
              "cast.name.bigramed^5"
              "directors.name.bigramed"]
     :type :best_fields
     :tie_breaker 0.4}}
   :size 10})

;; ---

(search
  {:query
   {:multi_match
    {:query "star trek patrick stewart"
     :fields ["title" "overview"
              "cast.name.bigramed"
              "directors.name.bigramed"]
     :type :most_fields}}
   :size 10})

;; ---

(search
  {:query
   {:multi_match
    {:query "star trek patrick stewart"
     :fields ["title^0.2" "overview"
              "cast.name.bigramed"
              "directors.name.bigramed"]
     :type :most_fields}}
   :size 10})

;; ---

(search
  {:query
   {:multi_match
    {:query "star trek patrick stewart william shatner"
     :fields ["title" "overview"
              "cast.name.bigramed"
              "directors.name.bigramed"]
     :type :most_fields}}
   :size 10})
