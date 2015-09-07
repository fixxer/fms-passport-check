(ns tests
    (:use clojure.test fixxer.fms.index))

(let [istreamf #(java.io.ByteArrayInputStream. (byte-array 5 [1 2 3 4 5]))]
  (deftest byte-seq-test
    (is (= (take 5 (byte-seq (istreamf))) '(1 2 3 4 5))))
  (deftest byte-seq-eof-test
    (is (= (take 6 (byte-seq (istreamf))) '(1 2 3 4 5)))))

(deftest merged-seq-test
  (is (= (merged-seq [1 3] [2 4] compare) '(1 2 3 4))))

(deftest partition-into-sorted-sets-test
  (is (= (partition-into-sorted-sets 2 [3 2 5 4 1])
         '(#{2 3} #{4 5} #{1}))))

(deftest parse-line-test
	(is (= (parse-line "1234;567890")
			[1234 567890])))
