(ns fixxer.fms.index
  (:import [com.google.common.hash BloomFilter Funnels]
           [fixxer.fms Util])
  (:require [clojure.java.io :as io])
  (:gen-class))

(defn parse-line [line]
  "Get numbers from string"
  (map #(Integer/valueOf %)
       (re-seq #"\d+" line)))

(defn create-bloom []
  "Create BloomFilter for byte[]"
  (BloomFilter/create (Funnels/byteArrayFunnel) 1000))

(defn pack-passport [series number]
  (Util/packPassport series number))

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

(defn merge-files [file1-name file2-name]
  "Merge two files"
  (with-open [is1 (io/input-stream file1-name)
              is2 (io/input-stream file2-name)]
    ))

(defn byte-seq [is]
  "Create byte sequence from InputStream"
  (let [the-byte (.read is)]
    (cond (= the-byte -1) nil
    :else (cons the-byte (lazy-seq (byte-seq is))))))

(defn merged-seq [seq1 seq2 cmp]
  "Merges two sorted sequences into lazy sequence"
  (cond
   (empty? seq1) seq2
   (empty? seq2) seq1
   :else
   (let [h1 (first seq1) h2 (first seq2)]
     (if (apply cmp [h1 h2])
       (cons h1
             (lazy-seq
              (merged-seq (rest seq1) seq2 cmp)))
       (cons h2
             (lazy-seq
              (merged-seq seq1 (rest seq2) cmp)))))))

(defn main [ & [file-name]]
  (with-open [rdr (io/reader file-name)
              bloom-ostream (io/output-stream "bloom.bin")]
      (->> (line-seq rdr)
           (map parse-line)
           (partition 10000)
           (map #(into (sorted-set) %))
           (map-indexed (fn [idx portion] [(str "segment-" idx ".bin") portion]))
           (apply write-portion))))
