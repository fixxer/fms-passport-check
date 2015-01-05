(ns tests
    (:use clojure.test fixxer.fms.index))

(deftest test-byte-seq
  (let [istream (java.io.ByteArrayInputStream. (byte-array 5 [1 2 3 4 5]))]
    (is (= (take 5 (byte-seq istream)) '(1 2 3 4 5)))))

(deftest test-byte-seq-eof
  (let [istream (java.io.ByteArrayInputStream. (byte-array 5 [1 2 3 4 5]))]
    (is (= (take 6 (byte-seq istream)) '(1 2 3 4 5)))))

(deftest test-merged-seq1
  (is (= (merged-seq [1 3] [2 4] <) '(1 2 3 4))))
