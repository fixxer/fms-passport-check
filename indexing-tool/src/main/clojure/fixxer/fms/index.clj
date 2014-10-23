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

(defn main [ & [file-name]]
  (with-open [rdr (clojure.java.io/reader file-name)
              bloom-ostream (clojure.java.io/output-stream "bloom.bin")]
      (->> (line-seq rdr)
           (map parse-line)
           (partition 10000)
           (map #(into (sorted-set) %))
           (map vector (map #(str "segment-" % ".bin")
                            (counter-seq)))
           (apply write-portion))))
