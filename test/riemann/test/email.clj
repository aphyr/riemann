(ns riemann.test.email
  (:use [riemann.time :only [unix-time]]
        riemann.email
        clojure.test))

(deftest subject
         (let [s #'riemann.email/subject]
           (are [events subject] (= (s events) subject)
                [] ""
                
                [{}] ""
                
                [{:host "foo"}] "foo"
                
                [{:host "foo"} {:host "bar"}] "foo and bar"
                
                [{:host "foo"} {:host "bar"} {:host "baz"}]
                "foo, bar, baz"
                
                [{:host "foo"} {:host "baz"} {:host "bar"} {:host "baz"}]
                "foo, baz, bar"

                [{:host 1} {:host 2} {:host 3} {:host 4} {:host 5}]
                "5 hosts"
                
                [{:host "foo" :state "ok"}] "foo ok"
                
                [{:host "foo" :state "ok"} {:host "bar" :state "ok"}] 
                "foo and bar ok"
               
                [{:host "foo" :state "error"} {:host "bar" :state "ok"}]
                "foo and bar error and ok"
                )))

(deftest override-formatting-test
         (let [a (promise)]
           (with-redefs [postal.core/send-message #(deliver a [%1 %2])]
             (email-event {} {:body (fn [events]
                                      (apply str "body " 
                                             (map :service events)))
                              :subject (fn [events] 
                                         (apply str "subject " 
                                                (map :service events)))}
                          {:service "foo"}))
           (is (= @a [{} {:subject "subject foo"
                          :body    "body foo"}]))))

(deftest ^:email ^:integration email-test
         (let [email (mailer {:from "riemann-test"})
               stream (email "aphyr@aphyr.com")]
           (stream {:host "localhost"
                    :service "email test"
                    :state "ok"
                    :description "all clear, uh, situation normal"
                    :metric 3.14159
                    :time (unix-time)})))
