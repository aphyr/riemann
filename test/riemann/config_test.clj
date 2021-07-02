(ns riemann.config-test
  (:require [riemann.common :refer [event]]
            [riemann.config :refer :all]
            [riemann.core :as core]
            [riemann.index :refer [Index]]
            [riemann.logging :as logging]
            [riemann.pubsub :as pubsub]
            [riemann.service :as service]
            [riemann.streams :as streams]
            [clojure.test :refer :all])
  (:import (java.util.concurrent RejectedExecutionException)))

; (set! *print-level* 4)

(defn reset-core! [f]
  (logging/suppress ["riemann.core"
                     "riemann.service"
                     "riemann.pubsub"]
                    (clear!)
                    (core/stop! @core)
                    (reset! core (core/core))
                    (f)
                    (core/stop! @core)
                    (clear!)
                    (reset! core (core/core))
                    (core/stop! @core)))

(use-fixtures :each reset-core!)

(deftest blank-test
  (is (empty? (:streams @core)))
  (is (empty? (:streams @next-core)))
  (is (every? true? (map service/equiv?
                         (:services @core)
                         (:services (core/core)))))
  (is (every? true? (map service/equiv?
                         (:services @next-core)
                         (:services (core/core))))))

(deftest apply-test
  (is (not= @core @next-core))
  (let [old-next-core @next-core]
    (apply!)
    (is (= (dissoc old-next-core :pubsub :services)
           (dissoc @core :pubsub :services)))
    (is (not= @core @next-core))))

(defn verify-service
  "Tests that the given service a.) is a service, b.) is in the next core, and
  c.) is not in the current core."
  [s]
  ; Is a service
  (is (satisfies? service/Service s))

  ; Not present in current core
  (is (not-every? (comp #{s}) (:services @core)))

  ; Present in next core
  (is (some #{s} (:services @next-core))))

(deftest service-test
  (let [sleep (fn [_] (Thread/sleep 1))]
    (testing "Adds an equivalent service to a core."
      (let [s1 (service! (service/thread-service :foo sleep))]
        (satisfies? service/Service s1)
        (apply!)
        (is (some #{s1} (:services @core)))
        (is (deref (:running s1)))

                                        ; Now add an equivalent service
        (let [s (service/thread-service :foo sleep)
              s2 (service! s)]
          (is (= s1 s2))
          (is (not= s s2))
          (apply!)
          (is (deref (:running s1)))
          (is (not (deref (:running s))))
          (is (some #{s1} (:services @core)))
          (is (not (some #{s} (:services @core)))))))

    (testing "Adds a distinct service to a core."
      (let [defaults (set (:services @next-core))
            s1 (service! (service/thread-service :foo sleep))]
        (satisfies? service/Service s1)
        (apply!)
        (is (some #{s1} (:services @core)))
        (is (deref (:running s1)))

        ; Add a distinct service. S1 should shut down since
        ; it is no longer present in the new target core.
        (let [s2 (service! (service/thread-service :bar sleep))]
          (is (not= s1 s2))
          (apply!)
          (is (not (deref (:running s1))))
          (is (deref (:running s2)))
          (is (some #{s2} (:services @core)))
          (is (not (some #{s1} (:services @core)))))))))

(deftest instrumentation-test
  (let [s (verify-service (instrumentation {:interval 2
                                            :enabled? false}))]
    (is (= [2000 false] (:equiv-key s)))))

(deftest tcp-server-test
  (verify-service (tcp-server :host "a"))
  (reset-core! #())
  (verify-service (tcp-server {:host "a"})))

(deftest udp-server-test
  (verify-service (udp-server :host "b"))
  (reset-core! #())
  (verify-service (udp-server {:host "b"})))

(deftest ws-server-test
  (verify-service (ws-server :port 1234))
  (reset-core! #())
  (verify-service (ws-server {:port 1234})))

(deftest graphite-server-test
  (verify-service (graphite-server :port 1))
  (reset-core! #())
  (verify-service (graphite-server {:port 1})))

(deftest streams-test
  (streams :a)
  (streams :b)
  (is (= [:a :b] (:streams @next-core)))
  (is (empty? (:streams @core))))

(deftest index-test
  (let [i (index)]
    (testing "index creation helper creates the index properly"
      (is (satisfies? Index i)))
    (testing "index creation helper sets the index of the current core"
      (is (= i (:index @core))))
    (testing "index creation helper sets the index of the next core"
      (is (= i (:index @next-core))))
    (testing "current core index is preserved after apply"
      (apply!)
      (is (identical? i (:index @core))))
    (testing "we have the proper reference to the index after a reload"
      (let [i' (index)]
        (is (identical? i' (:index @next-core)))
        (is (identical? i' (:index @core)))
        (apply!)
        (is (identical? i' (:index @core)))))))

(deftest update-index-test
  (let [i (index)]
    (apply!)
    (i {:service 1 :state "ok" :time 0})
    (is (= (seq i) [{:service 1 :state "ok" :time 0}]))
    (testing "ensure that index state persists through reloads"
      (let [i' (index)]
        (apply!)
        (is (= (seq i') [{:service 1 :state "ok" :time 0}]))))))

(deftest delete-from-index-test
  (let [i (index)
        delete (delete-from-index)
        states [{:host 1 :state "ok" :time 0}
                {:host 2 :state "ok" :time 0}
                {:host 1 :state "bad" :time 0}]]
    (apply!)
    (dorun (map i states))
    (delete {:host 1 :state "definitely not seen before"})
    (is (= (seq i) [{:host 2 :state "ok" :time 0}]))))

(deftest delete-from-index-fields
  (let [i (index)
        delete (delete-from-index [:host :state])]
    (apply!)
    (i {:host 1 :state "foo" :time 0})
    (i {:host 2 :state "bar" :time 0})
    (delete {:host 1 :state "not seen"})
    (delete {:host 2 :state "bar"})
    (is (= (seq i) [{:host 1 :state "foo" :time 0}]))))

(deftest async-queue-test
  (let [out    (atom [])
        p      (promise)
        stream (async-queue! :test {}
                             (fn [event]
                               (swap! out conj event)
                               (deliver p nil)))]
    (is (thrown? RejectedExecutionException
                 (stream :foo)))
    (apply!)
    (stream :bar)
    (deref p)
    (is (= [:bar] @out))))

(deftest reinject-test
  (let [event (promise)]
    (streams
     (streams/where (service "bar")
                    (partial deliver event))
     (streams/where (service "foo")
                    (streams/with :service "bar" reinject)))
    (apply!)
    (core/stream! @core {:service "foo" :metric 2 :time 0})
    (is (= (deref event 10 :timeout) {:service "bar" :metric 2 :time 0}))))

(deftest subscribe-in-stream-test
  (let [received (promise)]
    (streams
     (streams/where (service "test-in")
                    (publish :test))
     (subscribe :test (partial deliver received)))
    (apply!)

    ; Send through streams
    ((first (:streams @core)) {:service "test-in"})
    (is (= {:service "test-in"} @received))))

(deftest subscribe-outside-stream-test
  (let [received (promise)]
    (subscribe :test (partial deliver received))
    (apply!)

    ; Send outside streams
    (pubsub/publish! (:pubsub @core) :test "hi")
    (is (= "hi" @received))))

(deftest index-pubsub-test
  (let [received (promise)
        index (index)]
    (subscribe "index" (partial deliver received))
    (streams index)
    (apply!)
    (core/stream! @core {:service "foo" :time 0})
    (is (= {:service "foo" :time 0} (deref received 500 :timeout)))))
