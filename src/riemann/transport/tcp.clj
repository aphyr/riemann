(ns riemann.transport.tcp
  "Accepts messages from external sources. Associated with a core. Sends
  incoming events to the core's streams, queries the core's index for states."
  (:import [java.net InetSocketAddress]
           [java.util.concurrent Executors]
           [java.nio.channels ClosedChannelException]
           [org.jboss.netty.bootstrap ServerBootstrap]
           [org.jboss.netty.buffer ChannelBuffers]
           [org.jboss.netty.channel ChannelHandler
                                    ChannelHandlerContext
                                    ChannelPipeline
                                    ChannelPipelineFactory
                                    ChannelStateEvent
                                    Channels
                                    ExceptionEvent
                                    MessageEvent
                                    SimpleChannelHandler]
           [org.jboss.netty.channel.group ChannelGroup]
           [org.jboss.netty.channel.socket.nio NioServerSocketChannelFactory]
           [org.jboss.netty.handler.codec.frame LengthFieldBasedFrameDecoder
                                                LengthFieldPrepender]
           [org.jboss.netty.handler.execution
            OrderedMemoryAwareThreadPoolExecutor])
  (:use [riemann.transport :only [handle 
                                  protobuf-decoder
                                  protobuf-encoder
                                  msg-decoder
                                  msg-encoder
                                  shared-execution-handler
                                  channel-group
                                  channel-pipeline-factory]]
        [riemann.service :only [Service ServiceEquiv]]
        [clojure.tools.logging :only [info warn]]
        [riemann.transport :only [handle]]))

(defn int32-frame-decoder
  []
  ; Offset 0, 4 byte header, skip those 4 bytes.
  (LengthFieldBasedFrameDecoder. Integer/MAX_VALUE, 0, 4, 0, 4))

(defn int32-frame-encoder
  []
  (LengthFieldPrepender. 4))

(defn gen-tcp-handler
  "Wraps netty boilerplate for common TCP server handlers. Given a reference to
  a core, a channel group, and a handler fn, returns a SimpleChannelHandler
  which calls (handler core message-event) with each received message."
  [core ^ChannelGroup channel-group handler]
  (proxy [SimpleChannelHandler] []
    (channelOpen [context ^ChannelStateEvent state-event]
      (.add channel-group (.getChannel state-event)))

    (messageReceived [^ChannelHandlerContext context
                      ^MessageEvent message-event]
        (try
          (handler @core message-event)
          (catch java.nio.channels.ClosedChannelException e
            (warn "channel closed"))))
    
    (exceptionCaught [context ^ExceptionEvent exception-event]
      (let [cause (.getCause exception-event)]
        (when-not (instance? ClosedChannelException cause)
          (warn (.getCause exception-event) "TCP handler caught")
          (.close (.getChannel exception-event)))))))

(defn tcp-handler
  "Given a core and a MessageEvent, applies the message to core."
  [core ^MessageEvent e]
  (.write (.getChannel e)
          (handle core (.getMessage e))))

(defrecord TCPServer [host port channel-group pipeline-factory core killer]
  ; core is a reference to a core
  ; killer is a reference to a function which shuts down the server.

  ; TODO compare pipeline-factory!
  ServiceEquiv
  (equiv? [this other]
          (and (instance? TCPServer other)
               (= host (:host other))
               (= port (:port other))))
  
  Service
  (reload! [this new-core]
           (reset! core new-core))

  (start! [this]
          (locking this
            (when-not @killer
              (let [boss-pool (Executors/newCachedThreadPool)
                    worker-pool (Executors/newCachedThreadPool)
                    bootstrap (ServerBootstrap.
                                (NioServerSocketChannelFactory.
                                  boss-pool
                                  worker-pool))]

                ; Configure bootstrap
                (doto bootstrap
                  (.setPipelineFactory pipeline-factory)
                  (.setOption "readWriteFair" true)
                  (.setOption "tcpNoDelay" true)
                  (.setOption "reuseAddress" true)
                  (.setOption "child.tcpNoDelay" true)
                  (.setOption "child.reuseAddress" true)
                  (.setOption "child.keepAlive" true))

                ; Start bootstrap
                (let [server-channel (.bind bootstrap
                                            (InetSocketAddress. host port))]
                  (.add channel-group server-channel))
                (info "TCP server" host port "online")

                ; fn to close server
                (reset! killer 
                        (fn []
                          (-> channel-group .close .awaitUninterruptibly)
                          (.releaseExternalResources bootstrap)
                          (.shutdown worker-pool)
                          (.shutdown boss-pool)
                          (info "TCP server" host port "shut down")))))))

  (stop! [this]
         (locking this
           (when @killer
             (@killer)
             (reset! killer nil)))))

(defn tcp-server
  "Create a new TCP server. Doesn't start until (service/start!). Options:
  :host             The host to listen on (default 127.0.0.1).
  :port             The port to listen on. (default 5555)
  :core             An atom used to track the active core for this server
  :channel-group    A global channel group used to track all connections.
  :pipeline-factory A ChannelPipelineFactory for creating new pipelines."
  ([]
   (tcp-server {}))
  ([opts]
     (let [core          (get opts :core (atom nil))
           host          (get opts :host "127.0.0.1")
           port          (get opts :port 5555)
           channel-group (get opts :channel-group
                              (channel-group 
                                (str "tcp-server " host ":" port)))
           pf (get opts :pipeline-factory
                    (channel-pipeline-factory
                               int32-frame-decoder (int32-frame-decoder)
                      ^:shared int32-frame-encoder (int32-frame-encoder)
                      ^:shared executor            shared-execution-handler
                      ^:shared protobuf-decoder    (protobuf-decoder)
                      ^:shared protobuf-encoder    (protobuf-encoder)
                      ^:shared msg-decoder         (msg-decoder)
                      ^:shared msg-encoder         (msg-encoder)
                      ^:shared handler             (gen-tcp-handler 
                                                     core
                                                     channel-group
                                                     tcp-handler)))]
       (TCPServer. host port channel-group pf core (atom nil)))))
