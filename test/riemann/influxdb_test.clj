(ns riemann.influxdb-test
  (:require
    [clojure.test :refer :all]
    [riemann.influxdb :as influxdb]
    [riemann.logging :as logging]
    [riemann.time :refer [unix-time]]))

(logging/init)

; Would the next person working on influxdb kindly update these tests to use
; riemann.test-utils/with-mock? Would be nice to have something besides just
; integration tests. --Kyle, Sep 2015 :)

(deftest ^:influxdb-8 ^:integration influxdb-test-8
  (let [k (influxdb/influxdb {:block-start true})]
    (k {:host "riemann.local"
        :service "influxdb test"
        :state "ok"
        :description "all clear, uh, situation normal"
        :metric -2
        :time (unix-time)}))

  (let [k (influxdb/influxdb {:block-start true})]
    (k {:service "influxdb test"
        :state "ok"
        :description "all clear, uh, situation normal"
        :metric 3.14159
        :time (unix-time)}))

  (let [k (influxdb/influxdb {:block-start true})]
    (k {:host "no-service.riemann.local"
        :state "ok"
        :description "Missing service, not transmitted"
        :metric 4
        :time (unix-time)})))


(deftest ^:influxdb-9 ^:integration influxdb-test-9
  (let [k (influxdb/influxdb
            {:version :0.9
             :host (System/getenv "INFLUXDB_HOST")
             :db "riemann_test"})]
    (k {:host "riemann.local"
        :service "influxdb test"
        :state "ok"
        :description "all clear, uh, situation normal"
        :metric -2
        :time (unix-time)})
    (k {:service "influxdb test"
        :state "ok"
        :description "all clear, uh, situation normal"
        :metric 3.14159
        :time (unix-time)})
    (k {:host "no-service.riemann.local"
        :state "ok"
        :description "Missing service, not transmitted"
        :metric 4
        :time (unix-time)})))


(deftest point-conversion
  (is (nil? (influxdb/event->point-9 #{} {:service "foo test", :time 1}))
      "Event with no metric is converted to nil")

  (is (= {"measurement" "test service"
          "time" 1428366765
          "tags" {"host" "host-01"}
          "fields" {"value" 42.08}}
         (influxdb/event->point-9
           #{:host}
           {:host "host-01"
            :service "test service"
            :time 1428366765
            :metric 42.08}))
      "Minimal event is converted to point fields")
  
  (is (= {"measurement" "test service"
          "time" 14283499995
          "tags" {"host" "host-01"}
          "fields" {"value" 42.08}}
         (influxdb/event->point-9
           #{:host}
           {:host "host-01"
            :measurement "test service"
            :time 14283499995
            :metric 42.08}))
      "Event with measurement provided instead of service")

  (is (= {"measurement" "service_api_req_latency"
          "time" 1428354941
          "tags" {"host" "www-dev-app-01.sfo1.example.com"
                  "sys" "www"
                  "env" "dev"
                  "role" "app"
                  "loc" "sfo1"}
          "fields" {"value" 0.8025
                    "description" "A text description!"
                    "state" "ok"
                    "foo" "frobble"}}
         (influxdb/event->point-9
           #{:host :sys :env :role :loc}
           {:host "www-dev-app-01.sfo1.example.com"
            :service "service_api_req_latency"
            :time 1428354941
            :metric 0.8025
            :state "ok"
            :description "A text description!"
            :ttl 60
            :tags ["one" "two" "red"]
            :sys "www"
            :env "dev"
            :role "app"
            :loc "sfo1"
            :foo "frobble"}))
      "Full event is converted to point fields")

  (is (empty? (influxdb/events->points-9 #{} [{:service "foo test"}]))
      "Nil points are filtered from result"))


(deftest get-write-url 
  (is (= "http://localhost:8086/write?db=riemann&precision=s"
      (influxdb/get-write-url-9 {:db "riemann"
                        :scheme "http"
                        :host "localhost"
                        :port 8086}))
      "get-write-url-9 defaults to using seconds when unspecified")

  (is (= "http://localhost:8086/write?db=riemann&precision=ns"
         (influxdb/get-write-url-9 {:db "riemann"
                        :scheme "http"
                        :host "localhost"
                        :port 8086
                        :precision "ns"}))
      ))


(deftest line-protocol
  (is (= "some\\ service,host=ugly\"host\\=name,another=ta\\,g value=0.123456789,a_tag=\"a value\" 1442548067"
         (influxdb/lineprotocol-encode-9
           {"measurement" "some service"
            "time"        1442548067000/1000
            "tags"
              {"host"     "ugly\"host=name"
               "another"  "ta,g"}
            "fields"
              {"value"    0.123456789
               "a_tag"    "a value"}}))
    "Point field is converted to line encoding"))
