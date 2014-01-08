(ns riemann.index
  "Maintains a stateful index of events by [host, service] key. Can be queried
  to return the most recent indexed events matching some expression. Can expire
  events which have exceeded their TTL. Presently the only implementation of
  the index protocol is backed by a nonblockinghashmap, but I plan to add an
  HSQLDB backend as well.

  Indexes must extend three protocols:

  Index: indexing and querying events
  Seqable: returning a list of events
  Service: lifecycle management"
  (:require [riemann.query :as query])
  (:use [riemann.time :only [unix-time]]
         riemann.service)
  (:import (org.cliffc.high_scale_lib NonBlockingHashMap)))

(defprotocol Index
  (clear [this]
    "Resets the index")
  (delete [this event]
    "Deletes any event with this host & service from index. Returns the deleted
    event, or nil.")
  (delete-exactly [this event]
    "Deletes event from index. Returns the deleted event, or nil.")
  (delete-with [this pred]
    "Deletes events matching a given predicate")
  (expire [this]
    "Return a seq of expired states from this index, removing each.")
  (search-with [this pred]
    "Returns a seq of events matching a given predicate")
  (search [this query-ast]
    "Returns a seq of events from the index matching this query AST")
  (update [this event]
    "Updates index with event")
  (lookup [this host service]
    "Lookup an indexed event from the index"))

; The index accepts states and maintains a table of the most recent state for
; each unique [host, service]. It can be searched for states matching a query.

(def default-ttl 60)

(defn nbhm-index
  "Create a new nonblockinghashmap backed index"
  []
  (let [hm (NonBlockingHashMap.)]
    (reify
      Index
      (clear [this]
             (.clear hm))

      (delete [this event]
              (.remove hm [(:host event) (:service event)]))

      (delete-exactly [this event]
                      (.remove hm [(:host event) (:service event)] event))

      (delete-with [this pred]
        (doseq [event (search-with this pred)]
          (delete this event)))

      (expire [this]
              (filter
                (fn [{:keys [ttl time] :or {ttl default-ttl} :as state}]
                  (let [age (- (unix-time) time)]
                    (when (> age ttl)
                      (delete this state)
                      true)))
                (.values hm)))

      (search-with [this pred]
        "O(n), sadly."
        (filter pred (.values hm)))

      (search [this query-ast]
        (search-with this (query/fun query-ast)))

      (update [this event]
        (this event))

      (lookup [this host service]
        (.get hm [host service]))

      clojure.lang.IFn
      (invoke [this event]
        (when-not (= "expired" (:state event))
          (.put hm [(:host event) (:service event)] event)
          event))

      clojure.lang.Seqable
      (seq [this]
           (seq (.values hm)))

      ServiceEquiv
      (equiv? [this other] (= (class this) (class other)))

      Service
      (conflict? [this other] false)
      (reload! [this new-core])
      (start! [this])
      (stop! [this]))))

(defn index
  "Create a new index (currently: an nhbm index)"
  []
  (nbhm-index))
