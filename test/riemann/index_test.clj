(ns riemann.index-test
  (:refer-clojure :exclude [update])
  (:use riemann.index
        riemann.core
        riemann.query
        [riemann.instrumentation :only [events]]
        [riemann.common :only [event]]
        [riemann.time :only [unix-time]]
        clojure.test)
  (:require [riemann.service :as service]))

(deftest missing-time-throws
  (riemann.logging/suppress
   ["riemann.core"]
   (let [i (wrap-index (index))
         e (event {:host 1 :service 2})]
     (is (thrown-with-msg? clojure.lang.ExceptionInfo
                           #"^cannot index event.*$"
                           (i {:host 1 :service 2})))
     (i e)
     (is (= (set i) #{e})))))

(deftest nbhm-update
         (let [i (wrap-index (index))]
           (i {:host 1 :time 0})
           (i {:host 2 :time 0})
           (i {:host 1 :service 3 :state :ok :time 0})
           (i {:host 1 :service 3 :description "new" :time 0})

           (is (= (set i)
                  #{{:host 1 :time 0}
                    {:host 2 :time 0}
                    {:host 1 :service 3 :description "new"  :time 0}}))))

(deftest nhbm-delete
         (let [i (wrap-index (index))]
           (i {:host 1 :time 0})
           (i {:host 2 :time 0})
           (delete i {:host 1 :service 1})
           (delete i {:host 2 :state :ok})
           (is (= (set i)
                  #{{:host 1 :time 0}}))))

(deftest nhbm-search
         (let [i (wrap-index (index))]
           (i {:host 1 :time 0})
           (i {:host 2 :service "meow" :time 0})
           (i {:host 3 :service "mrrrow" :time 0})
           (is (= (set (search i (ast "host >= 2 and not service =~ \"%r%\"")))
                  #{{:host 2 :service "meow" :time 0}}))))

(deftest nhbm-expire
         (let [i (wrap-index (index))]
           (i {:host 1 :ttl 0 :time  (dec (unix-time))})
           (i {:host 2 :ttl 10 :time (unix-time)})
           (i {:host 3 :ttl 20 :time (- (unix-time) 21)})
           ; Default TTLs
           (i {:host 4 :ttl nil :time (unix-time)})
           (i {:host 5 :ttl nil :time (- (unix-time) default-ttl 1)})

           (let [expired (expire i)]
             (is (= (set (map :host expired))
                    #{1 3 5})))

           (is (= (set (map :host i))
                  #{2 4}))))

(deftest nbhm-read-index
         (let [i (wrap-index (index))]
           (i {:host 1 :service 1 :metric 5 :time 0})
           (i {:host 1 :service 2 :metric 7 :time 0})

           (is (= 5 (:metric (lookup i 1 1))))
           (is (= 7 (:metric (lookup i 1 2))))))

(deftest nbhm-instrumentation
  (let [i (wrap-index (index))]

    (i {:host 1 :service 1 :metric 5 :time 0})
    (i {:host 1 :service 2 :metric 7 :time 0})

    (is (= 2 (:metric (first (filter #(= (:service %) "riemann index size") (events i))))))))

(defn random-event
  [& {:as event}]
  (merge {:host    (rand-int 100)
          :service (rand-int 100)
          :ttl     (rand-int 500)
          :time    (- (unix-time) (rand-int 30))}
         event))

(deftest ^:bench indexing-nbhm-time
  (let [_        (println "building events, this might take some time")
        not-much (doall (repeatedly 100 random-event))
        a-few    (doall (repeatedly 100000 random-event))
        a-lot    (doall (repeatedly 1000000 random-event))
        i        (wrap-index (nbhm-index))]
    (println "updating and expiring the same 100 events 10000 times:")
    (time (dotimes [iter 10000]
            (do (doseq [event not-much]
                  (i event)))))
    (println "expiring")
    (time (dotimes [iter 10000] (doall (expire i))))
    (clear i)

    (println "updating and expiring the same 100000 events 100 times:")
    (time (dotimes [iter 100]
            (do (doseq [event a-few]
                  (i event)))))
    (println "expiring")
    (time (dotimes [iter 100] (doall (expire i))))
    (clear i)

    (println "updating and expiring the same 10000000 events 10 times:")
    (time (dotimes [iter 10]
            (do (doseq [event a-lot]
                  (i event)))))
    (println "expiring")
    (time (dotimes [iter 10] (doall (expire i))))


    (println "updating and expiring the same 100 events 10000 times (8 threads):")
    (time (doall (pmap (fn [_] (dotimes [iter 10000]
            (do (doseq [event not-much]
                  (i event))))) (range 8))))
    (clear i)

    (println "updating and expiring the same 100000 events 100 times (8 threads):")
    (time (doall (pmap (fn [_] (dotimes [iter 100]
            (do (doseq [event a-few]
                  (i event))))) (range 8))))
    (clear i)

    (println "updating and expiring the same 10000000 events 10 times (8 threads):")
    (time (doall (pmap (fn [_] (dotimes [iter 10]
            (do (doseq [event a-lot]
                  (i event))))) (range 8))))

    ))


(deftest query-for-host-and-service-test
  (testing "not matching"
    (let [ast (ast "metric = 4")]
      (is (= nil (query-for-host-and-service ast)))))

  (testing "matching"
    (let [ast (ast "host = nil and service = \"ser\"")]
      (is (= [nil "ser"] (query-for-host-and-service ast)))))

  (testing "with more"
    (let [ast (ast "host = nil and service = \"ser\" and metric > 5")]
      (is (= nil (query-for-host-and-service ast)))))

  (testing "non-list sub-predicates"
    (let [ast (ast "host = 2 and metric")]
      (is (= nil (query-for-host-and-service ast)))))

  (testing "two hosts"
    (let [ast (ast "host = \"h1\" and host = \"h2\"")]
      (is (= nil (query-for-host-and-service ast))))))

(deftest service-interface
  (testing "service equivalance of indexes"
    (let [one-index (index)
          two-index (index)]
      (is (service/equiv? one-index two-index))))

  (testing "service equivalance of wrapped indexes"
    (let [one-index (wrap-index (index))
          two-index (wrap-index (index))]
      (is (service/equiv? one-index two-index))))

  (testing "service equivalance of wrapped to unwrapped index"
    (let [one-index (wrap-index (index))
          two-index (index)]
      (is (not (service/equiv? one-index two-index))))))
