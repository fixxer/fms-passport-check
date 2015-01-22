(ns fixxer.fms.index
  (:import [com.google.common.hash BloomFilter Funnels]
           [fixxer.fms Util]
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

(defn pack-passport [series number]
  "Pack passport series and number into 5 bytes"
  (let [int-to-byte-array (fn [n] (.. (ByteBuffer/allocate 4) (putInt series) array))
        sbytes (int-to-byte-array series)
        nbytes (int-to-byte-array number)]
    (byte-array 5 [(aget sbytes 2)
                   (aget sbytes 3)
                   (aget nbytes 1)
                   (aget nbytes 2)
                   (aget nbytes 3)])))

(defn write-portion [file-name portion]
  "Writes portion into file"
  (with-open [o (io/output-stream file-name)]
    (doseq [passport portion]
      (.write o (apply pack-passport (take 2 passport))))))

(defn merge-sorted [acc fst snd cmp]
  "Merge two sorted vectors"
  (cond
   (empty? fst) (into [] (concat acc snd))
   (empty? snd) (into [] (concat acc fst))
   (< (cmp fst snd) 0) (recur (conj acc (first fst))
                              (into [] (rest fst))
                              snd
                              cmp)
   :else (recur (conj acc (first snd))
                fst
                (into [] (rest snd))
                cmp)))

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

(defn merge-streams [istream1 istream2 ostream]
  "Merge sorted 5-byte streams"
  (doseq [e (merged-seq
               (partition 5 (byte-seq istream1))
               (partition 5 (byte-seq istream2))
               compare)]
      (.write ostream (byte-array 5 e))))

(defn merge-files [file1-name file2-name out-file-name]
  "Merge two sorted files"
  (with-open [is1 (io/input-stream file1-name)
              is2 (io/input-stream file2-name)
              os (io/output-stream out-file-name)]
    (merge-streams is1 is2 os)))

(defn partition-into-sorted-sets [n xs]
  "Partitions sequence `xs` into sorted sets size `n`"
  (let [into-sorted-set (fn [ys] (into (sorted-set) ys))]
    (map into-sorted-set (partition n n [] xs))))

(defn reduce-segments [[out-file in-file1] in-file2]
  (do (merge-files in-file1 in-file2 out-file)
    [in-file1 out-file]))

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
                  (reduce reduce-segments ["temp.bin" (first segments)] (last segments)))))]
    (sparse-index 10000 (getChannel. idx-stream))))
