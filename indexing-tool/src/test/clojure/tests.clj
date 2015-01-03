(ns tests
    (:use clojure.test fixxer.fms.index))

(deftest test-byte-seq
  (let [istream (java.io.ByteArrayInputStream. (byte-array 5 [1 2 3 4 5]))]
    (is (= (take 5 (byte-seq istream)) '(1 2 3 4 5)))))
