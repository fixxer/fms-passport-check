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

(defn byte-seq [istream]
  "Create byte sequence from InputStream"
  (let [the-byte (.read istream)]
    (cond (= the-byte -1) nil
    :else (cons the-byte (lazy-seq (byte-seq istream))))))

(defn merged-seq [xs ys cmp]
  "Merges two sorted sequences into lazy sequence"
  (cond
   (empty? xs) ys
   (empty? ys) xs
   :else (let [x (first xs) y (first ys)]
     (if (< (apply cmp [x y]) 0)
       (cons x (lazy-seq (merged-seq (rest xs) ys cmp)))
       (cons y (lazy-seq (merged-seq xs (rest ys) cmp)))))))

(defn partition-into-sorted-sets [n xs]
  "Partitions sequence `xs` into sorted sets size `n`"
  (let [into-sorted-set (fn [ys] (into (sorted-set) ys))]
    (map into-sorted-set (partition n n [] xs))))

(defn main [ & [file-name]]
  (with-open [rdr (io/reader file-name)]
      (->> (line-seq rdr)
           (map parse-line)
           (partition 10000)
           (map #(into (sorted-set) %))
           (map-indexed (fn [idx portion] [(str "segment-" idx ".bin") portion]))
           (apply write-portion))))
