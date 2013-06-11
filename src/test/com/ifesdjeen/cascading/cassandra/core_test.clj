(ns com.ifesdjeen.cascading.cassandra.core-test
  (:use cascalog.api
        clojure.test
        cascalog.playground
        [midje sweet cascalog])
  (:require [cascalog.io :as io]
            [cascalog.ops :as c]
            [clj-hector.core :as hector]
            [clj-hector.ddl :as ddl])
  (:import [cascading.tuple Fields]
           [cascading.scheme Scheme]
           [org.apache.cassandra.service EmbeddedCassandraService]
           [org.apache.cassandra.thrift CassandraDaemon]
           [com.ifesdjeen.cascading.cassandra CassandraTap CassandraScheme]
           [com.ifesdjeen.cascading.cassandra.hadoop SerializerHelper]
           [org.apache.cassandra.utils ByteBufferUtil]
           [org.apache.cassandra.db.marshal TypeParser UTF8Type IntegerType CompositeType]
           [org.apache.cassandra.thrift Column]
           [java.nio ByteBuffer]
           [org.apache.log4j Logger Level]))

;; turn down verbose test output
(-> (Logger/getLogger "org.apache.cassandra") (.setLevel Level/WARN))
(-> (Logger/getLogger "me.prettyprint.cassandra") (.setLevel Level/WARN))

;; preamble for embedded cassandra
(def ^:dynamic *cassandra-config* "src/resources/cassandra.yaml")
(def ^:dynamic *cassandra-tmp* "/tmp/ci-cassandra")
(def cluster "Test Cluster")
(def keyspace "test")

;; managing embedded cassandra
(defn rmr [f]
  (if (.isDirectory f) (dorun (map rmr (.listFiles f))) (.delete f)) (.delete f))

(defn cleanup-cassandra []
  (rmr (java.io.File. *cassandra-tmp*)))

(defn start-embedded-cassandra []
  (System/setProperty "cassandra.config" (str (.toURI (java.io.File. *cassandra-config*))))
  (.start (EmbeddedCassandraService.)))

(defn stop-embedded-cassandra []
  (try
    (CassandraDaemon/stop (into-array String nil)) ;; have to produce a String[] for the static method call
    (catch NullPointerException ex nil))
  (.addShutdownHook (Runtime/getRuntime) (Thread. cleanup-cassandra)))

;; convenience macro
(defmacro with-embedded-cassandra
  [& body]
  `(do
     (start-embedded-cassandra)
     (try ~@body (finally (stop-embedded-cassandra)))))

;; memoize connections
(defn make-connection
  []
   (hector/cluster cluster "localhost" 9999))

(def connect! (memoize make-connection))

;; seriously?
(defn keyspace!
  [ks]
   (hector/keyspace (connect!) ks))

(def connect! (memoize make-connection))

(defn create-keyspace
  []
  (ddl/add-keyspace (connect!) {:name keyspace :strategy "SimpleStrategy"}))

(defn- make-column
  [[n type]]
  {:name (name n) :validator type})

(defn- make-columns
  [d]
  (into [] (map make-column d)))

(defn create-cf
  [name columns]
  (ddl/add-column-family (connect!) keyspace {:name name :column-metadata (make-columns columns)}))

(defn reset-schema!
  []
  (try
  (ddl/drop-keyspace (connect!) keyspace)
  (catch Exception e nil))
  (create-keyspace)
  (create-cf "libraries" {:language :utf-8
                          :schmotes :integer
                          :votes :integer})
  (create-cf "composites" {:value :composite}))

(defn create-tap
  [cf conf]
  (let [defaults      {"db.columnFamily" cf
                       "sink.keyColumnName" "name"
                       "db.host" "localhost"
                       "db.port" "9999"
                       "db.keyspace" keyspace
                       "cassandra.inputPartitioner" "org.apache.cassandra.dht.RandomPartitioner"
                       "cassandra.outputPartitioner" "org.apache.cassandra.dht.RandomPartitioner"}
        scheme        (CassandraScheme. (merge defaults conf))
        tap           (CassandraTap. scheme)]
    tap))

(defn seed-data
  [ks cf n]
  (dotimes [counter n]
    (hector/put ks cf (str "Cassaforte" counter)
           {"language" (str "Clojure" counter)
            "schmotes" (int counter)
            "votes" (int counter)})))

(def test-composite-data
  [["row1" 100 1]
   ["row2" 200 2]])

(def test-composite-output
  [["row1" {[100 "value"] 1}]
   ["row2" {[200 "value"] 2}]])

(defn unpack-composite
  [[composite colname]]
  [(SerializerHelper/deserialize composite "LongType") (SerializerHelper/deserialize colname "UTF8Type")])

(defn unpack
  [row]
  (let [id (first (keys row))
        row (row id)
        columns (keys row)
        handler (TypeParser/parse "CompositeType(LongType,UTF8Type)")
        composites (map #(.split handler (ByteBuffer/wrap %)) columns)
        composites (map unpack-composite composites)
        values (apply hash-map (interleave composites (vals row)))]
  [id values]))

(with-embedded-cassandra
  (let [ks (keyspace! keyspace)
        cf "libraries"
        tap (create-tap cf {"source.types"
                            {"language"    "UTF8Type"
                            "schmotes"    "Int32Type"
                            "votes"       "Int32Type"}})
        composite-tap (create-tap "composites"
                        {"sink.compositeColumns" ["composite"]
                         "sink.keyColumnName" "id"
                         "sink.sinkImpl" "com.ifesdjeen.cascading.cassandra.CompositeRowSink"
                         "sink.compositeColumnTypes" ["LongType"]
                         "sink.outputMappings" {"id" "?id"
                                                "composite" "?composite"
                                                "value" "?value"}})]
    (reset-schema!)
    (seed-data ks cf 100)

    (fact "Handles simple calculations"
          (<-
           [?count ?sum]
           (tap ?value1 ?value2 ?value3 ?value4)
           (c/count ?count)
           (c/sum ?value3 :> ?sum))
          => (produces [[100 4950]]))

    (?<- composite-tap [?id ?composite ?value]
      (test-composite-data ?id ?composite ?value))

    (fact "Outputs composite columns"
      (map unpack (hector/get-rows ks "composites" ["row1" "row2"] :v-serializer :long)) =>
        (just test-composite-output :in-any-order))))
