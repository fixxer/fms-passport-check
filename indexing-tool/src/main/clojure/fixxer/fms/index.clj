(ns fixxer.fms.index
  (:import [com.google.common.hash BloomFilter Funnels]
           [fixxer.fms Util])
  (:gen-class))

(defn parse-line [line]
  "Get numbers from string"
  (map #(Integer/valueOf %)
       (re-seq #"\d+" line)))

(defn create-bloom []
  "Create BloomFilter for byte[]"
  (BloomFilter/create (Funnels/byteArrayFunnel) 1000))

(defn main [ & [file-name]]
  (with-open [rdr (clojure.java.io/reader file-name)
              bloom-ostream (clojure.java.io/output-stream "bloom.txt")]
    (let [bloom (create-bloom)]
      (doseq [pasport (map parse-line (line-seq rdr))]
        (.put bloom (apply Util/packPassport pasport)))
      (.writeTo bloom bloom-ostream))))
