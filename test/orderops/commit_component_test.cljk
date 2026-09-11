(ns orderops.commit-component-test
  (:require [clojure.test :refer [deftest is testing]]
            [kotoba.compiler.core :as compiler]
            [kotoba.kir :as ir]))

(def ^:private source
  (slurp "src/orderops/commit.kotoba"))

(defn- compile-kir []
  (-> (compiler/check-source source {:allow #{[:cap/call 12]}})
      :hir
      ir/lower))

(deftest commit-is-one-typed-conditional-storage-effect
  (let [kir (compile-kir)
        function (first (filter #(= 'commit (:name %)) (:functions kir)))
        calls (filter #(and (seq? %) (= 'typed-cap-call (first %)))
                      (tree-seq coll? seq (:body function)))]
    (is (= 1 (count calls)))
    (is (= 12 (second (first calls))))
    (is (= [:ref :orderops.storage/request] (nth (first calls) 2)))
    (is (= [:ref :orderops.storage/result] (nth (first calls) 3)))))

(deftest conflict-is-an-explicit-result-not-a-retry-loop
  (let [kir (compile-kir)
        function (first (filter #(= 'commit (:name %)) (:functions kir)))
        result-cases (get-in kir [:schemas :orderops.storage/result 2])]
    (is (= [:written :conflict-current :conflict-missing :error]
           (mapv first result-cases)))
    (is (= 1 (count (filter #(and (seq? %) (= 'typed-cap-call (first %)))
                            (tree-seq coll? seq (:body function)))))
        "the guest contains no retrying second write")))

(deftest source-packages-as-a-standard-component
  (let [artifact
        (compiler/compile-component
         source {:allow #{[:cap/call 12]}}
         {:component-abilities
          {12 {:target "kotobase://marketplace-order"
               :operation :storage/transact
               :max-bytes 65536
               :max-items 1
               :deadline-ms 5000
               :audit-id "marketplace-order-commit-v1"}}})]
    (is (= :wasm-component/v1 (:format artifact)))
    (is (= #{:aiueos.component/aiueos-storage-transact}
           (:capabilities artifact)))
    (is (= [:storage/transact] (get-in artifact [:wit :imports])))
    (testing "no ambient WASI surface is introduced"
      (is (empty? (filter #(re-find #"^wasi:" (name %))
                          (get-in artifact [:wit :imports])))))))
