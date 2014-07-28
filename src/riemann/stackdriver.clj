(ns riemann.stackdriver
  "Forwards events to Stackdriver."
  (:require riemann.time
            [org.httpkit.client :as client]
            [cheshire.core :refer [generate-string]]
            [clojure.string :as str]))

(def gateway-url "https://custom-gateway.stackdriver.com/v1/custom")

(defn metric-name
  "Constructs a metric name from an event."
  [opts event]
  (let [service ((:name opts) event)]
     (str/replace service #"\s+" ".")))

(defn generate-datapoint
  "Accepts riemann event/events and converts it into equivalent stackdriver datapoint."
  [opts event-or-events]
  (let [events (if (sequential? event-or-events) event-or-events (list event-or-events))]
    (map (fn [event]
      {:name (metric-name opts event)
       :value (:metric event)
       :collected_at (long (:time event))})
    events)))

(defn post-datapoint
  "Post the riemann metrics datapoints."
  [api-key uri data]
  (let [http-options {:body data
                      :headers {"x-stackdriver-apikey" api-key
                                "Content-Type" "application/json"}}]
    ;; fire and forget
    (client/post uri http-options)))

(defn stackdriver
  "Returns a function which accepts an event/events and sends it to Stackdriver."
  [opts]
  (let [ts (atom 0)
        opts (merge {:api-key "stackdriver-api-key"
                     :name :service} opts)]
    (fn [event]
      (let [post-data {:timestamp (swap! ts #(max (inc %) (long (riemann.time/unix-time))))
                       :proto_version 1
                       :data (generate-datapoint opts (remove (comp nil? :metric) event))}
            json-data (generate-string post-data)]
        (post-datapoint (:api-key opts) gateway-url json-data)))))
