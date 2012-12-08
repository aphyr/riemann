(ns riemann.test.streams
  (:use riemann.streams
        [riemann.common :exclude [match]]
        riemann.time.controlled
        riemann.time
        clojure.test)
  (:require [riemann.index :as index]
            [riemann.folds :as folds]
            incanter.stats))

(use-fixtures :once control-time!)
(use-fixtures :each reset-time!)

(defmacro run-stream
  "Applies inputs to stream, and returns outputs."
  [stream inputs]
  `(let [out# (ref [])
         stream# (~@stream (append out#))]
     (doseq [e# ~inputs] (stream# e#))
     (deref out#)))

(defmacro run-stream-intervals
  "Applies a seq of alternating events and intervals (in seconds) between them to stream, returning outputs."
  [stream inputs-and-intervals]
  `(let [out# (ref [])
         stream# (~@stream (append out#))
         start-time# (ref (unix-time))
         next-time# (ref (deref start-time#))]
     (doseq [[e# interval#] (partition-all 2 ~inputs-and-intervals)]
       (stream# e#)
       (when interval#
         (dosync (ref-set next-time# (+ (deref next-time#) interval#)))
         (advance! (deref next-time#))))
     (let [result# (deref out#)]
       ; close stream
       (stream# {:state "expired"})
       result#)))

(defmacro test-stream
  "Verifies that the given stream, taking inputs, forwards outputs to children."
  [stream inputs outputs]
  `(is (= (run-stream ~stream ~inputs) ~outputs)))

(defmacro test-stream-intervals
  "Verifies that run-stream-intervals, taking inputs/intervals, forwards outputs to chldren."
  [stream inputs-and-intervals outputs]
  `(is (= (run-stream-intervals ~stream ~inputs-and-intervals) ~outputs)))

(defn evs
  "Generate events based on the given event, with given metrics"
  [base-event & metrics]
  (vec (map #(assoc base-event :metric %)
            metrics)))

(defn em
  "Generate events with given metrics"
  [& metrics]
  (vec (map (fn [m] {:metric m}) metrics)))

(deftest combine-test
         (let [r (ref nil)
               sum (combine folds/sum (register r))
               min (combine folds/minimum (register r))
               max (combine folds/maximum (register r))
               mean (combine folds/mean (register r))
               median (combine folds/median (register r))
               events [{:metric 1}
                       {:metric 0}
                       {:metric -2}]]
           (sum events)
           (is (= (deref r) {:metric -1}))
           (min events)
           (is (= (deref r) {:metric -2}))
           (max events)
           (is (= (deref r) {:metric 1}))
           (mean events)
           (is (= (deref r) {:metric -1/3}))
           (median events)
           (is (= (deref r) {:metric 0}))))

(deftest smap-test
         (test-stream (smap inc) [6 3 -1] [7 4 0]))

(deftest sreduce-test
         (testing "explicit value"
                  (test-stream (sreduce + 1) [1 2 3] [2 4 7]))
         
         (testing "implicit value"
                  (test-stream (sreduce +) [1 2 3 4] [3 6 10])))

(deftest counter-test
         (let [r      (ref [])
               s      (counter (append r))
               events [{:metric 2}
                       {}
                       {:metric 1}
                       {:metric 5}
                       {:tags ["reset"] :metric -1}
                       {:metric 2}]]
           (doseq [e events] (s e))
           
           (is (= (deref r)
                  [{:metric 2}
                   {:metric 3}
                   {:metric 8}
                   {:tags ["reset"] :metric -1}
                   {:metric 1}]))))

(deftest match-test
         ; Regular strings.
         (test-stream (match :service "foo")
                      [{}
                       {:service "bar"}
                       {:service "foo"}]
                      [{:service "foo"}])

         ; Sets
         (test-stream (match :metric #{0 2})
                      [{}
                       {:metric 1}
                       {:metric 2}]
                      [{:metric 2}])

         ; Regexen
         (test-stream (match :state #"^mi")
                      [{}
                       {:state "migas"}
                       {:state "other breakfast foods"}]
                      [{:state "migas"}])

         ; Functions
         (test-stream (match identity 2)
                      [1 2 3]
                      [2]))

(deftest tagged-all-test
         (test-stream (tagged-all ["kitten" "cat"])
                      [{:tags ["kitten" "cat"]}
                       {:tags ["kitten", "cat", "meow"]}
                       {:tags ["dog" "cat"]}
                       {:tags ["cat"]}
                       {:tags []}
                       {}]
                      [{:tags ["kitten" "cat"]}
                       {:tags ["kitten", "cat", "meow"]}])

         (test-stream (tagged-all "meow")
                      [{:tags ["meow" "bark"]}
                       {:tags ["meow"]}
                       {:tags ["bark"]}
                       {}]
                      [{:tags ["meow" "bark"]}
                       {:tags ["meow"]}]))

(deftest tagged-any-test
         (test-stream (tagged-any ["kitten" "cat"])
                      [{:tags ["kitten" "cat"]}
                       {:tags ["cat", "dog"]}
                       {:tags ["kitten"]}
                       {:tags ["dog"]}
                       {:tags []}
                       {}]
                      [{:tags ["kitten" "cat"]}
                       {:tags ["cat", "dog"]}
                       {:tags ["kitten"]}])

         (test-stream (tagged-all "meow")
                      [{:tags ["meow" "bark"]}
                       {:tags ["meow"]}
                       {:tags ["bark"]}
                       {}]
                      [{:tags ["meow" "bark"]}
                       {:tags ["meow"]}]))

(deftest where*-test
         (test-stream (where* identity)
                      [true false nil 2]
                      [true 2])

         (test-stream (where* expired?)
                      [{:time -1 :ttl 0.5}
                       {:time 0 :ttl 1}]
                      [{:time -1 :ttl 0.5}])

         ; Complex closure with else clause
         (let [good (atom [])
               bad  (atom [])
               s (where* (fn [event]
                           (or (= "good" (:service event))
                               (< 2 (:metric event))))
                         (partial swap! good conj)
                         (else (partial swap! bad conj)))
               events [{:service "good" :metric 0}
                       {:service "bad" :metric 0}
                       {:metric 1}
                       {:service "bad" :metric 1}
                       {:service "bad" :metric 3}]]

           ; Run stream
           (doseq [e events] (s e))

           (is (= @good 
                  [{:service "good" :metric 0}
                   {:service "bad" :metric 3}]))
           (is (= @bad
                  [{:service "bad" :metric 0}
                   {:metric 1}
                   {:service "bad" :metric 1}]))))

(deftest where-field
         (let [r (ref [])
               s (where (or (state "ok" "good")
                            (= "weird" state))
                        (fn [e] (dosync (alter r conj e))))
               events [{:state "ok"}
                       {:state "good"}
                       {:state "weird"}
                       {:state "error"}]
               expect [{:state "ok"}
                       {:state "good"}
                       {:state "weird"}]]
           (doseq [e events] (s e))
           (is (= expect (deref r)))))

(deftest where-regex
         (test-stream (where (service #"^foo"))
                      [{}
                       {:service "foo"}
                       {:service "food"}]
                      [{:service "foo"}
                       {:service "food"}]))

(deftest where-variable
         ; Verify that the macro allows variables to be used in predicates.
         (let [regex #"cat"]
           (test-stream (where (service regex))
                        [{:service "kitten"}
                         {:service "cats"}]
                        [{:service "cats"}])))

(deftest where-tagged
         (let [r (ref [])
               s (where (tagged "foo") (append r))
               events [{}
                       {:tags []}
                       {:tags ["blah"]}
                       {:tags ["foo"]}
                       {:tags ["foo" "bar"]}]]
           (doseq [e events] (s e))
           (is (= (deref r)
                  [{:tags ["foo"]} {:tags ["foo" "bar"]}]))))

(deftest where-else
         ; Where should take an else clause.
         (let [a (atom [])
               b (atom [])]
           (run-stream
             (where (service #"a")
                    #(swap! a conj (:service %))
                    (else #(swap! b conj (:service %))))
             [{:service "cat"}
              {:service "dog"}
              {:service nil}
              {:service "badger"}])
           (is (= @a ["cat" "badger"]))
           (is (= @b ["dog" nil]))))

(deftest where-child-evaluated-once
         ; Where should evaluate its children exactly once.
         (let [x (atom 0)
               s (where true (do (swap! x inc) identity))]
           (is (= @x 1))
           (s {:service "test"})
           (is (= @x 1))
           (s {:service "test"})
           (is (= @x 1))))

(deftest default-kv
         (let [r (ref nil)
               s (default :service "foo" (register r))]
           (s {:service nil})
           (is (= {:service "foo"} (deref r)))

           (s {:service "foo"})
           (is (= {:service "foo"} (deref r)))

           (s {:service "bar" :test "baz"})
           (is (= {:service "bar" :test "baz"} (deref r)))))

(deftest default-map
         (let [r (ref nil)
               s (default {:service "foo" :state nil} (register r))]
           (s (event {:service nil}))
           (is (= "foo" (:service (deref r))))
           (is (= nil (:state (deref r))))

           (s (event {:service "foo"}))
           (is (= "foo" (:service (deref r))))
           (is (= nil (:state (deref r))))

           (s (event {:service "bar" :host "baz" :state "evil"}))
           (is (= "bar" (:service (deref r))))
           (is (= "baz" (:host (deref r))))
           (is (= "evil" (:state (deref r))))))

(deftest with-kv
         (let [r (ref nil)
               s (with :service "foo" (fn [e] (dosync (ref-set r e))))]
           (s {:service nil})
           (is (= {:service "foo"} (deref r)))

           (s {:service "foo"})
           (is (= {:service "foo"} (deref r)))

           (s {:service "bar" :test "baz"})
           (is (= {:service "foo" :test "baz"} (deref r)))))

(deftest with-map
         (let [r (ref nil)
               s (with {:service "foo" :state nil} (fn [e] (dosync (ref-set r e))))]
           (s (event {:service nil}))
           (is (= "foo" (:service (deref r))))
           (is (= nil (:state (deref r))))

           (s (event {:service "foo"}))
           (is (= "foo" (:service (deref r))))
           (is (= nil (:state (deref r))))

           (s (event {:service "bar" :test "baz" :state "evil"}))
           (is (= "foo" (:service (deref r))))
           (is (= nil (:state (deref r))))))

(deftest by-single
         ; Each test stream keeps track of the first host it sees, and confirms
         ; that each subsequent event matches that host.
         (let [i (ref 0)
               s (by :host
                     (let [host (ref nil)]
                       (fn [event]
                         (dosync
                           (alter i inc)
                           (when (nil? (deref host))
                             (ref-set host (event :host)))
                           (is (= (deref host) (event :host)))))))
               events (map (fn [h] {:host h}) [:a :a :b :a :c :b])]
           (doseq [event events]
             (s event))
           (is (= (count events) (deref i)))))

(deftest by-multiple
         ; Each test stream keeps track of the first host/service it sees, and
         ; confirms that each subsequent event matches that host.
         (let [i (ref 0)
               s (by [:host :service]
                     (let [host (ref nil)
                           service (ref nil)]
                       (fn [event]
                         (dosync
                           (alter i inc)

                           (when (nil? (deref host))
                             (ref-set host (event :host)))
                           (when (nil? (deref service))
                             (ref-set service (event :service)))

                           (is (= (deref host) (event :host)))
                           (is (= (deref service) (event :service)))))))

               events (map (fn [h] {:host (first h)
                                    :service (last h)})
                           [[1 :a]
                            [1 :b]
                            [1 :a]
                            [2 :a]
                            [2 :a]
                            [1 :a]
                            [2 :a]
                            [1 :b]])]
           (doseq [event events]
             (s event))
           (is (= (count events) (deref i)))))

(deftest by-evaluates-children-once-per-branch
         (let [i (atom 0)
               s (by :metric (do (swap! i inc) identity))]
           (is (= @i 0))
           (s {:metric 1})
           (is (= @i 1))
           (s {:metric 2})
           (is (= @i 2))
           (s {:metric 1})
           (is (= @i 2))))

(deftest fill-in-test
         (test-stream-intervals
           (fill-in 0.01 {:metric 0})
           []
           [])

         ; Quick succession
         (is (= (map :metric (run-stream-intervals
                               (fill-in 0.01 {:metric 0})
                               (interpose nil (em 1 2 3))))
                [1 2 3]))

         ; With a gap and expiry
         (is (= (map :metric (run-stream-intervals
                               (fill-in 0.05 {:metric 0})
                               [{:metric 1} 0.06
                                {:metric 2} nil
                                {:metric 3} 0.08
                                {:metric 4 :state "expired"} 0.06
                                {:metric 5}]))
                [1 0 2 3 0 4 5]))
         )

(deftest fill-in-last-test
  (test-stream-intervals
    (fill-in-last 0.01 {:metric 0})
    []
    [])

  ; Quick succession
  (let [output (run-stream-intervals
                 (fill-in-last 0.01 {:metric 0})
                 (interpose nil (evs {:host "foo" :service "bar"}
                                     1 2 3)))]
    (is (= (map :metric output)
           [1 2 3]))
    (is (= (map :host output)
           ["foo" "foo" "foo"]))
    (is (= (map :service output)
           ["bar" "bar" "bar"])))

  ; With a gap and expiry
  (let [output (run-stream-intervals
                 (fill-in-last 0.05 {:metric 0})
                 [{:host "a" :metric 1} 0.06
                  {:host "b" :metric 2} nil
                  {:host "c" :metric 3} 0.08
                  {:host "d" :metric 4 :state "expired"} 0.06
                  {:host "e" :metric 5}])]
    (is (= (map :metric output)
           [1 0 2 3 0 4 5]))
    (is (= (map :host output)
           ["a" "a" "b" "c" "c" "d" "e"]))))

(deftest interpolate-constant-test
         (test-stream-intervals 
           (interpolate-constant 0.01)
           []
           [])

         ; Should forward a single state
         (is (= (map :metric (run-stream-intervals
                               (interpolate-constant 0.1)
                               [{:metric 1} 0.05]))
                [1]))

         ; Should forward first state and ignore immediate successors
         (is (= (map :metric (run-stream-intervals
                               (interpolate-constant 0.1)
                               [{:metric 1} 0.05 
                                {:metric 2} nil
                                {:metric 3}]))
                [1]))

         ; Should fill in missing states regularly
         (is (= (map :metric (run-stream-intervals 
                               (interpolate-constant 0.1)
                               (interpose 0.22 (em 1 2 3 4))))
                [1 1 1 2 2 3 3]))
         
         ; Should forward final "expired" state.
         (is (= (map :metric (run-stream-intervals
                               (interpolate-constant 0.1)
                               (interpose 0.22
                                          [{:metric 1}
                                           {:metric 2}
                                           {:metric 3}
                                           {:metric 4 :state "expired"}])))
                [1 1 1 2 2 3 3 4]))

         ; Should not fill during expired times.
         (is (= (map :metric (run-stream-intervals
                               (interpolate-constant 0.05)
                               [{:metric 1 :state "expired"}
                                0.12
                                {:metric 2}
                                0.12
                                {:metric 3 :state "expired"}
                                0.12]))
                [1 2 2 2 3]))
         )

(deftest ddt-immediate-test
         ; Empty -> empty
         (test-stream (ddt) [] [])
         ; Ignore stream without metrics
         (test-stream (ddt) [{} {} {} {}] [])
         ; Do nothing the first time
         (test-stream (ddt) [{:metric 1 :time 0}] [])
         ; Differentiate
         (test-stream (ddt) 
                      [{:metric 0 :time 0}
                       {:metric 0 :time 1}
                       {:metric 2 :time 2}
                       {:metric -4 :time 4}]
                      [{:metric 0 :time 1}
                       {:metric 2 :time 2}
                       {:metric -3 :time 4}]))

(deftest ddt-interval-test
         ; Quick burst without crossing interval
         (is (= (map :metric (run-stream-intervals 
                               (ddt 0.1)
                               [{:metric 1} nil {:metric 2} nil {:metric 3}]))
                []))

         ; 1 event per interval
         (let [t0 (unix-time)]
           (is (= (map :metric (run-stream-intervals
                                 (ddt 0.1)
                                 [{:metric -1 :time t0} 0.1
                                  {:metric 0 :time (+ 1/10 t0)} 0.1
                                  {:metric -5 :time (+ 2/10 t0)} 0.1]))
                  [10 -50])))
        
         (reset-time!)

         ; n events per interval
         (let [t0 (unix-time)]
           (is (= (map :metric (run-stream-intervals
                                 (ddt 0.1)
                                 [{:metric -1 :time t0} 0.01 ; counts
                                  {:metric 100 :time (+ 1/20 t0)} 0.05
                                  {:metric 1 :time (+ 2/20 t0)} 0.05
                                  {:metric nil :time (+ 3/20 t0)} 0.05
                                  {:metric -3 :time (+ 4/20 t0)} 0.05]))
                  [20 -40])))
         )

(deftest rate-slow-even
         (let [output (ref [])
               interval 1
               intervals 5
               gen-rate 10
               total (* gen-rate intervals)
               gen-period (/ interval gen-rate)
               r (rate interval
                        (fn [event] (dosync (alter output conj event))))]

           ; Generate events
           (dotimes [_ intervals]
             (dotimes [_ gen-rate]
               (advance! (+ (unix-time) gen-period))
               (r {:metric 1 :time (unix-time)})))

           ; Give all futures time to complete
           (advance! (+ (unix-time) gen-period))

           ; Verify output states
           (let [o (deref output)]
             
             ; All events recorded
             (is (approx-equal total (reduce + (map :metric o))))

             ; Middle events should have the correct rate.
             (is (every? (fn [measured-rate] 
                           (approx-equal gen-rate measured-rate))
                         (map :metric (drop 1 (drop-last o)))))
           
             ; First and last events should be complementary
             (let [first-last (+ (:metric (first o))
                                 (:metric (last o)))]
               (is (or (approx-equal (* 2 gen-rate) first-last)
                       (approx-equal gen-rate first-last))))

             )))

(deftest rate-fast
         (let [output (atom [])
               interval 1
               total 10000
               threads 4
               r (rate interval
                        (fn [event] (dosync (swap! output conj event))))
               t0 (unix-time)]

           ; Generate events
           (doseq [f (map (fn [t] (future 
                                 (dotimes [i (/ total threads)]
                                   (r {:metric 1 :time (unix-time)}))))
                          (range threads))]
                   (deref f))
             
           (advance! interval)

           (let [t1 (unix-time)
                 duration (- t1 t0)]
           
             ; All events recorded
             (is (= duration interval))
             (is (approx-equal total (reduce + (map :metric @output))))

             )))

(deftest fold-interval-test
         (test-stream-intervals (riemann.streams/fold-interval 1 :metric incanter.stats/sd)
                                [{:metric 2} 0.1 {:metric 4} 0.2 {:metric 2} 0.3 {:metric 4} 1.0 {:metric 100} 0.1 {:metric 100} 1.0]
                                (em 1.1547005383792515 0.0)))

(deftest fold-interval-metric-test
         (test-stream-intervals (riemann.streams/fold-interval-metric 1 incanter.stats/sd)
                                [{:metric 2} 0.1 {:metric 4} 0.2 {:metric 2} 0.3 {:metric 4} 1.0 {:metric 100} 0.1 {:metric 100} 1.0]
                                (em 1.1547005383792515 0.0)))

(deftest changed-test
         (let [output (ref [])
               r (changed :state
                          (fn [event] (dosync (alter output conj event))))
               r2 (changed :state {:init :ok} 
                           (append output))
               states [:ok :bad :bad :ok :ok :ok :evil :bad]]
           
           ; Apply states
           (doseq [state states]
             (r {:state state}))

           ; Check output
           (is (= [:ok :bad :ok :evil :bad]
                  (vec (map (fn [s] (:state s)) (deref output)))))

           ; Test with init
           (dosync (ref-set output []))
           (doseq [state states]
             (r2 {:state state}))
           
           (is (= [:bad :ok :evil :bad]
                  (vec (map (fn [s] (:state s)) (deref output)))))))
           
(deftest changed-state-test
         ; Each test stream keeps track of the first host/service it sees, and
         ; confirms that each subsequent event matches that host, and that
         ; each event is different from the previous state.
         (let [i (ref 0)
               s (changed-state
                   (let [host (ref nil)
                         service (ref nil)
                         state (ref nil)]
                     (fn [event]
                       (dosync
                         (alter i inc)

                         (is (not= (deref state) (:state event)))
                         (ref-set state (:state event))

                         (when (nil? (deref host))
                           (ref-set host (event :host)))
                         (when (nil? (deref service))
                           (ref-set service (event :service)))

                         (is (= (deref host) (event :host)))
                         (is (= (deref service) (event :service)))))))

               events [{:host 1 :service 1 :state 1}
                       {:host 2 :service 1 :state 1}
                       {:host 1 :service 1 :state 1}
                       {:host 1 :service 1 :state 2}
                       {:host 2 :service 1 :state 2}
                       {:host 2 :service 2 :state 1}
                       {:host 2 :service 1 :state 1}]]

           (doseq [event events]
             (s event))
           (is (= 6 (deref i)))))
(deftest within-test
         (let [output (ref [])
               r (within [1 2]
                         (fn [e] (dosync (alter output conj e))))
               metrics [0.5 1 1.5 2 2.5]
               expect [1 1.5 2]]
           
           (doseq [m metrics] (r {:metric m}))
           (is (= expect (vec (map (fn [s] (:metric s)) (deref output)))))))

(deftest without-test
         (let [output (ref [])
               r (without [1 2]
                         (fn [e] (dosync (alter output conj e))))
               metrics [0.5 1 1.5 2 2.5]
               expect [0.5 2.5]]
           
           (doseq [m metrics] (r {:metric m}))
           (is (= expect (vec (map (fn [s] (:metric s)) (deref output)))))))

(deftest over-test
         (let [output (ref [])
               r (over 1.5
                         (fn [e] (dosync (alter output conj e))))
               metrics [0.5 1 1.5 2 2.5]
               expect [2 2.5]]
           
           (doseq [m metrics] (r {:metric m}))
           (is (= expect (vec (map (fn [s] (:metric s)) (deref output)))))))

(deftest under-test
         (let [output (ref [])
               r (under 1.5
                         (fn [e] (dosync (alter output conj e))))
               metrics [0.5 1 1.5 2 2.5]
               expect [0.5 1]]
           
           (doseq [m metrics] (r {:metric m}))
           (is (= expect (vec (map (fn [s] (:metric s)) (deref output)))))))

(deftest update-test
         (let [i (index/index)
               s (update-index i)
               states [{:host 1 :state "ok"} 
                       {:host 2 :state "ok"} 
                       {:host 1 :state "bad"}]]
           (doseq [state states] (s state))
           (is (= (set i)
                  #{{:host 1 :state "bad"}
                    {:host 2 :state "ok"}}))))

(deftest delete-from-index-test
         (let [i (index/index)
               s (update-index i)
               d (delete-from-index i)
               states [{:host 1 :state "ok"} 
                       {:host 2 :state "ok"} 
                       {:host 1 :state "bad"}]]
           (doseq [state states] (s state))
           (doseq [state states] (d state))
           (is (= (vec (seq i)) []))))

(deftest ewma-timeless-test
         (test-stream (ewma-timeless 0)
                      (em 1 10 20 -100 4)
                      (em 0 0  0   0   0))
         (test-stream (ewma-timeless 1)
                      (em 5 13 1 -10 3)
                      (em 5 13 1 -10 3))
         (test-stream (ewma-timeless 1/2)
                      (em 1   1   1   1     1    )
                      (em 1/2 3/4 7/8 15/16 31/32)))

(deftest throttle-test
         (let [out (ref [])
               quantum 0.1
               stream (throttle 5 quantum (append out))
               t1 (unix-time)]
           
           (doseq [state (take 100000 (repeat {:state "foo"}))]
             (stream state))

           (let [dt (- (unix-time) t1)
                 slices (inc (quot dt quantum))
                 maxcount (* slices 5)
                 count (count (deref out))]

             ; Depending on whether we fell exactly on the interval boundary...
             ; ugh I hate testing this shit
             (is (approx-equal count maxcount 0.01))
             (is (zero? (mod count 5))))))

(deftest rollup-test
         (let [out (ref [])
               quantum 0.1
               stream (rollup 2 quantum (append out))
               t1 (unix-time)]
          
           (stream 1)
           (is (= (deref out) [[1]]))
           (stream 2)
           (is (= (deref out) [[1] [2]]))
           (stream 3)
           (is (= (deref out) [[1] [2]]))

           (advance! (* 1 quantum))
           (is (= (deref out) [[1] [2] [3]]))

           (stream 4)
           (is (= (deref out) [[1] [2] [3] [4]]))
           (stream 5)
           (stream 6)
           (stream 7)
           (is (= (deref out) [[1] [2] [3] [4]]))

           (advance! (* 2 quantum))
           (is (= (deref out) [[1] [2] [3] [4] [5 6 7]]))))

(deftest coalesce-test
         (let [out (ref [])
               s (coalesce (register out))
               a {:service 1 :state "ok" :time (unix-time)}
               b {:service 2 :state "ok" :time (unix-time)}
               c {:service 1 :state "bad" :time (unix-time)}
               d {:service 1 :state "ok" :time (unix-time) :ttl 0.01}
               e {:service 3 :state "ok" :time (unix-time)}]

           (s a)
           (is (= (set (deref out)) #{a}))

           (s b)
           (is (= (set (deref out)) #{a b}))

           (s c)
           (is (= (set (deref out)) #{b c}))

           (s d)
           (is (= (set (deref out)) #{b d}))

           ; Wait for ttl expiry of d
           (advance! 0.02)

           (s e)
           (is (= (set (deref out)) #{b e}))))

(deftest adjust-test
  (let [out (ref nil)
        s (adjust [:state str " 2"] (register out))]

    (s {})
    (is (= (deref out) {:state " 2"}))

    (s {:state "hey" :service "bar"})
    (is (= (deref out) {:state "hey 2" :service "bar"})))

  (let [out (ref nil)
        s (adjust #(assoc % :metric (count (:tags %)))
                  (register out))]

    (s {:service "a" :tags []})
    (is (= (deref out) {:service "a" :tags [] :metric 0}))

    (s {:service "a" :tags ["foo" "bar"]})
    (is (= (deref out) {:service "a" :tags ["foo" "bar"] :metric 2}))))

(deftest moving-event-window-test
         ; Zero-width windows.
         (test-stream (moving-event-window 0) [] [])
         (test-stream (moving-event-window 0) [1 2] [[] []])

         ; n-width windows
         (test-stream (moving-event-window 2) [1 2 3] [[1] [1 2] [2 3]]))

(deftest fixed-event-window-test
         ; Zero-width windows.
         (test-stream (fixed-event-window 0) [] [])
         (test-stream (fixed-event-window 0) [1 2] [])

         ; n-width windows
         (test-stream (fixed-event-window 2) [1] [])
         (test-stream (fixed-event-window 2) [1 2] [[1 2]])
         (test-stream (fixed-event-window 2) [1 2 3 4 5] [[1 2] [3 4]]))

(deftest moving-time-window-test
         ; Zero-second windows.
         (test-stream (moving-time-window 0) [] [])
         (test-stream (moving-time-window 0) [{:time 1} {:time 2}] [])

         ; n-width windows
         (test-stream (moving-time-window 2) [] [])
         (test-stream (moving-time-window 2) [{:time 1}] [[{:time 1}]])
         (test-stream (moving-time-window 2) 
                      [{:time 1} {:time 2} {:time 3} {:time 4}]
                      [[{:time 1}]
                       [{:time 1} {:time 2}]
                       [{:time 2} {:time 3}]
                       [{:time 3} {:time 4}]])

         ; With out-of-order events
         (test-stream (moving-time-window 2)
                      [{:time 5}
                       {:time 1}
                       {:time 2}
                       {:time 6}
                       {:time 3}
                       {:time 8}
                       {:time 4}
                       {:time 8}
                       {:time 5}
                       {:time 9}]
                      [[{:time 5}]
                       [{:time 5} {:time 6}]
                       [{:time 8}]
                       [{:time 8} {:time 8}]
                       [{:time 8} {:time 8} {:time 9}]]))

(deftest fixed-time-window-test
         ; Zero-time windows.
         (is (thrown? IllegalArgumentException (fixed-time-window 0)))

         ; n-width windows
         (test-stream (fixed-time-window 2) [] [])
         (test-stream (fixed-time-window 2) [{:time 1}] [])
         (test-stream (fixed-time-window 2) 
                      [{:time 1} {:time 2} {:time 3} {:time 4} {:time 5}]
                      [[{:time 1} {:time 2}]
                       [{:time 3} {:time 4}]])

         ; With a gap
         (test-stream (fixed-time-window 2) [{:time 1} {:time 7}] 
                      [[{:time 1}] [] []])

         ; With out-of-order events
         (test-stream (fixed-time-window 2)
                      [{:time 5}
                       {:time 1}
                       {:time 2}
                       {:time 6}
                       {:time 3}
                       {:time 8}
                       {:time 4}
                       {:time 8}
                       {:time 5}
                       {:time 9}
                       {:time 11}]
                      [[{:time 5} {:time 6}]
                       [{:time 8} {:time 8}]
                       [{:time 9}]]))
