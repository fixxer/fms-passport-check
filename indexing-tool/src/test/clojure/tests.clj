(ns tests
    (:use clojure.test fixxer.fms.index))

(let [istreamf #(java.io.ByteArrayInputStream. (byte-array 5 [1 2 3 4 5]))]
  (deftest test-byte-seq
    (is (= (take 5 (byte-seq (istreamf))) '(1 2 3 4 5))))
  (deftest test-byte-seq-eof
    (is (= (take 6 (byte-seq (istreamf))) '(1 2 3 4 5)))))

(deftest test-merged-seq1
  (is (= (merged-seq [1 3] [2 4] compare) '(1 2 3 4))))

(deftest test-partition-into-sorted-sets
  (is (= (partition-into-sorted-sets 2 [3 2 5 4 1])
         '(#{2 3} #{4 5} #{1}))))
