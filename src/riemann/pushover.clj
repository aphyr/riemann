(ns riemann.pushover
  "Forwards events to Pushover."
  (:require [clj-http.client :as client]))

(def ^:private event-url
  "https://api.pushover.net/1/messages.json")

(defn- post
  "POST to Pushover."
  [params]
  (client/post event-url
               {:form-params
                params}))

(defn- pushover-priority-set
  "Convert riemann event state to pushover priority"
  [event]
  (case (str (:state event))
    "ok" 0
    "info" -2
    "warning" -1
    "error" 0
    "critical" 1
    "emergency" 2
    -1))

(defn- default-event-formatter
  "Formats an event for Pushover"
  [event]
  {:title (str (:host event) " " (:service event))
   :message (str (:host event) " "
                 (:service event) " is "
                 (:state event) " ("
                 (:metric event) ")")
   :priority (pushover-priority-set event)
   :timestamp (:time event)})

(defn pushover
  "Returns a function which accepts an event and sends it to Pushover.
  An options map can be provided as an optional third argument.

  Options:

  - :formatter Optional event formatter function

  For details on Pushover options see https://pushover.net/api

  Examples:

  ```clojure
  (pushover \"APPLICATION_TOKEN\" \"USER_KEY\")

  (pushover \"APPLICATION_TOKEN\" \"USER_KEY\" {:formatter my-custom-event-formatter})
  ```"
  ([token user]
   (pushover token user {}))
  ([token user opts]
   (fn [event]
     (let [opts (merge {:formatter default-event-formatter}
                       opts)
           pushover-event ((:formatter opts) event)
           pushover-params (dissoc (assoc opts
                                          :token token
                                          :user user
                                          :title (:title pushover-event)
                                          :message (:message pushover-event)
                                          :priority (str (:priority pushover-event 0))
                                          :expire (str (:expire pushover-event 2400))
                                          :retry (str (:retry pushover-event 600))
                                          :timestamp (str (:timestamp pushover-event) ""))
                                   :formatter)]
       (post pushover-params)))))
