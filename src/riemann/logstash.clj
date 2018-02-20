(ns riemann.logstash
  "Forwards events to LogStash."
  (:refer-clojure :exclude [replace])
  (:import
   (java.net Socket
             DatagramSocket
             DatagramPacket
             InetAddress)
   (java.io Writer OutputStreamWriter))
  (:use [clojure.string :only [split join replace]]
        clojure.tools.logging
        riemann.pool
        riemann.common
        less.awful.ssl))

(defprotocol LogStashClient
  (open [client]
        "Creates a LogStash client")
  (send-line [client line]
        "Sends a formatted line to LogStash")
  (close [client]
         "Cleans up (closes sockets etc.)"))

(defn get-socket
  [host  port  opts]
  (cond
     (= ^Boolean (:tls opts) true)
         (socket  (ssl-context (:key opts) (:cert opts) (:ca-cert opts))
                              host
                              port)
     :else
          (Socket. host port)))

(defrecord LogStashTCPClient [^String host ^int port opts]
  LogStashClient

  (open [this]

    (let [sock (get-socket host port opts)]

        (cond
           (= ^Boolean (:tls opts) true)
              (do
                (.getSession sock)
                (assoc this
                       :socket sock
                       :out (.getOutputStream sock))
              )
          :else
              (assoc this
                     :socket sock
                     :out (OutputStreamWriter. (.getOutputStream sock))))))

  (send-line [this line]
    (let [out (:out this)]
          (cond
            (= ^Boolean (:tls opts) true)
               (do
                (.write  out  (.getBytes (str line ) ))
                (.flush out))
            :else
              (do
                (.write ^OutputStreamWriter out ^String line)
                (.flush ^OutputStreamWriter out)))))

  (close [this]
    (cond
      (= ^Boolean (:tls opts) true)
         (do
            (.close (:out this))
            (.close ^Socket (:socket this)))
        :else
        (do
           (.close ^OutputStreamWriter (:out this))
           (.close ^Socket (:socket this))))))

(defrecord LogStashUDPClient [^String host ^int port]
  LogStashClient
  (open [this]
    (assoc this
           :socket (DatagramSocket.)
           :host host
           :port port))
  (send-line [this line]
    (let [bytes (.getBytes ^String line)
          length (count line)
          addr (InetAddress/getByName (:host this))
          datagram (DatagramPacket. bytes length ^InetAddress addr port)]
      (.send ^DatagramSocket (:socket this) datagram)))
  (close [this]
    (.close ^DatagramSocket (:socket this))))

(defn logstash
  "Returns a function which accepts an event and sends it to logstash.
  Silently drops events when logstash is down. Attempts to reconnect
  automatically every five seconds. Use:

  (logstash {:host \"logstash.local\" :port 2003})

  Options:

  :pool-size  The number of connections to keep open. Default 4.

  :reconnect-interval   How many seconds to wait between attempts to connect.
                        Default 5.

  :claim-timeout        How many seconds to wait for a logstash connection from
                        the pool. Default 0.1.

  :block-start          Wait for the pool's initial connections to open
                        before returning.

  :protocol             Protocol to use. Either :tcp (default) or :udp.

  TLS options:
  :tls?             Whether to enable TLS
  :key              A PKCS8-encoded private key file
  :cert             The corresponding public certificate
  :ca-cert          The certificate of the CA which signed this key"

  [opts]

  (let [opts (merge {:host "127.0.0.1"
                     :port 9999
                     :protocol :tcp
                     :tls false
                     :claim-timeout 0.1
                     :pool-size 4} opts)

        pool (fixed-pool
               (fn []
                 (info "Connecting to " (select-keys opts [:host :port :security.protocol :tls]))
                 (let [
                       host (:host opts)
                       port (:port opts)
                       client (open (condp = (:protocol opts)
                                      :tcp (LogStashTCPClient. host port opts)
                                      :udp (LogStashUDPClient. host port)))]
                   (info "Connected")
                   client))
               (fn [client]
                 (info "Closing connection to "
                       (select-keys opts [:host :port]))
                 (close client))
               {:size                 (:pool-size opts)
                :block-start          (:block-start opts)
                :regenerate-interval  (:reconnect-interval opts)})]

    (fn [event]
      (with-pool [client pool (:claim-timeout opts)]
                 (let [string (event-to-json (merge event {:source (:host event)}))]
                   (send-line client (str string "\n")))))))
