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

(defn write-portion [file-name portion]
  "Writes sorted portion into file"
  (with-open [o (output-stream file-name)]
    (doseq [passport portion]
      (write (apply Util/packPassport pasport)))))

(defn merge-sorted [acc fst snd cmp]
  "Merge two sorted vectors"
  (cond
   (empty? fst) (into [] (concat acc snd))
   (empty? snd) (into [] (concat acc fst))
   (< (cmp fst snd) 0) (recur (conj acc (first fst))
                              (into [] (rest fst))
                              snd)
   :else (recur (conj acc (first snd))
                fst
                (into [] (rest snd)))))

(defn main [ & [file-name]]
  (with-open [rdr (clojure.java.io/reader file-name)
              bloom-ostream (clojure.java.io/output-stream "bloom.bin")]
      (->> (line-seq rdr)
           (map parse-line)
           (partition 10000)
           (map #(into (sorted-set) %))
           (map-indexed (fn [idx portion] [(str "segment-" idx ".bin") portion]))
           (apply write-portion))))
