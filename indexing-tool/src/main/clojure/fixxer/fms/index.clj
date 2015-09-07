(ns fixxer.fms.index
  (:import [com.google.common.hash BloomFilter Funnels]
           [java.nio ByteBuffer])
  (:require [clojure.java.io :as io])
  (:gen-class :main true))

(defn parse-line [line]
  "Get numbers from string"
  (map #(Integer/valueOf %)
       (re-seq #"\d+" line)))

(defn create-bloom []
  "Create BloomFilter for byte[]"
  (BloomFilter/create (Funnels/byteArrayFunnel) 1000))

(defn int->byte-array [n]
  "Convert int to byte array"
  (.. (ByteBuffer/allocate 4) (putInt n) array))

(defn pack-passport [series number]
  "Pack passport series and number into 5 bytes"
  (let [sbytes (int->byte-array series)
        nbytes (int->byte-array number)]
    (byte-array 5 [(aget sbytes 2)
                   (aget sbytes 3)
                   (aget nbytes 1)
                   (aget nbytes 2)
                   (aget nbytes 3)])))

(defn packed-passports [xs]
  "Packed passport sequence"
  (map (fn [x] (apply pack-passport (take 2 x)) xs)))

(defn write-portion [file-name portion]
  "Writes portion into file"
  (with-open [o (io/output-stream file-name)]
    (doseq [passport portion]
      (.write o (apply pack-passport (take 2 passport))))))

(defn byte-seq [istream]
  "Create byte sequence from InputStream"
  (let [b (.read istream)]
    (cond (= b -1) nil
    :else (cons b (lazy-seq (byte-seq istream))))))

(defn merged-seq [xs ys cmp]
  "Merges two sorted sequences into lazy sequence"
  (cond
   (empty? xs) ys
   (empty? ys) xs
   :else (let [x (first xs) y (first ys)]
     (if (< (apply cmp [x y]) 0)
       (cons x (lazy-seq (merged-seq (rest xs) ys cmp)))
       (cons y (lazy-seq (merged-seq xs (rest ys) cmp)))))))

(defn merge-passport-seq [xs ys]
  "Merge sorted 5-byte sequences"
  (merged-seq (partition 5 xs) (partition 5 ys) compare))

(defn merge-files [file1-name file2-name out-file-name]
  "Merge two sorted files"
  (with-open [is1 (io/input-stream file1-name)
              is2 (io/input-stream file2-name)
              os (io/output-stream out-file-name)]
    (doseq [x (merge-passport-seq is1 is2)]
      (.write os (byte-array 5 x)))))

(defn partition-into-sorted-sets [n xs]
  "Partitions sequence `xs` into sorted sets size `n`"
  (let [into-sorted-set (fn [ys] (into (sorted-set) ys))]
    (map into-sorted-set (partition n n [] xs))))

(defn create-sparse-index-fn [sparse-factor]
  (fn [file-channel]
    ((fn internal [n]
       (let [buf (ByteBuffer/wrap (byte-array 5))
             bytes-read (.read file-channel buf n)]
         (if (= bytes-read -1) nil
           (cons [buf n] (lazy-seq (internal (+ n sparse-factor)))))))
     0)))

(defn sparse-index [sparse-factor file-channel]
  ((create-sparse-index-fn sparse-factor) file-channel))

(defn write-sparse-index [os idx]
  (doseq [[data pos] idx]
    (.write os data)
    (.write os pos)))

(defn -main [ & [file-name]]
  (with-open [idx-stream
              (io/input-stream
               (first
                (let [segments (with-open
                                 [rdr (io/reader file-name)]
                                 (->> (line-seq rdr)
                                      (map parse-line)
                                      (partition-into-sorted-sets 10000)
                                      (map-indexed (fn [idx part] [(str "segment-" idx ".bin") part]))
                                      (map (fn [[file-name data]]
                                             (do (write-portion file-name data)
                                               file-name)))))]
                  )))]
    (sparse-index 10000 (.getChannel idx-stream))))
