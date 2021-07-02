(ns riemann.config
  "Riemann config files are eval'd in the context of this namespace. Includes
  streams, client, email, logging, and graphite; the common functions used in
  config. Provides a default core and functions ((tcp|udp)-server, streams,
  index, reinject) which operate on that core."
  (:import (java.io File))
  (:require [riemann [boundary    :refer [boundary]]
                     [client      :refer [udp-client tcp-client multi-client]]
                     [cloudwatch  :refer [cloudwatch]]
                     [common      :as common :refer [event]]
                     [core        :as core]
                     [datadog     :refer [datadog]]
                     [druid       :refer [druid]]
                     [email       :refer [mailer]]
                     [folds       :as folds]
                     [graphite    :as graphite-client :refer [graphite]]
                     [hipchat     :refer [hipchat]]
                     [index       :as index]
                     [influxdb    :refer [influxdb]]
                     [influxdb2   :refer [influxdb2]]
                     [kafka       :as kafka :refer [kafka]]
                     [kairosdb    :refer [kairosdb]]
                     [keenio      :refer [keenio]]
                     [librato     :refer [librato-metrics]]
                     [logentries  :refer [logentries]]
                     [logging     :as logging]
                     [logstash    :as logstash :refer [logstash]]
                     [mailgun     :refer [mailgun]]
                     [msteams     :refer [msteams]]
                     [nagios      :refer [nagios]]
                     [netuitive   :refer [netuitive]]
                     [opentsdb    :refer [opentsdb]]
                     [opsgenie    :refer [opsgenie]]
                     [pagerduty   :refer [pagerduty]]
                     [plugin      :refer [load-plugin load-plugins]]
                     [pubsub      :as pubsub]
                     [pushover    :refer [pushover]]
                     [rabbitmq    :refer [rabbitmq]]
                     [repl]
                     [service     :as service]
                     [shinken     :refer [shinken]]
                     [slack       :refer [slack]]
                     [sns         :refer [sns-publisher]]
                     [stackdriver :refer [stackdriver]]
                     [prometheus  :refer [prometheus]]
                     [elasticsearch :refer [elasticsearch
                                            default-bulk-formatter
                                            elasticsearch-bulk]]
                     [streams     :refer :all]
                     [telegram    :refer [telegram]]
                     [test        :as test :refer [tap io tests]]
                     [time        :refer [unix-time linear-time once! every!]]
                     [twilio      :refer [twilio]]
                     [victorops   :refer [victorops]]
                     [xymon       :refer [xymon]]
                     [zabbix      :refer [zabbix]]]
            [riemann.transport [tcp        :as tcp]
                               [udp        :as udp]
                               [websockets :as websockets]
                               [sse        :as sse]
                               [graphite   :as graphite]
                               [opentsdb   :as opentsdb]
                               [rabbitmq   :as rabbitmq]]
            [cemerick.pomegranate :refer [add-dependencies]]
            [clojure.java.io :refer [file]]
            [clojure.tools.nrepl.server :as repl]
            [clojure.tools.logging :refer :all]))

(def core "The currently running core."
  (atom (core/core)))
(def next-core "The core which will replace the current core."
  (atom (core/core)))

(defn kwargs-or-map
  "Takes a sequence of arguments like

  [{:foo 2 :bar 3}]
  [:foo 2 :bar 3]

  as would be passed to a function taking either kwargs or an options map, and
  returns an options map."
  [opts]
  (if (and (= 1 (count opts))
           (map? (first opts)))
    (first opts)
    (apply array-map opts)))

(defn repl-server
  "Starts a new REPL server with opts."
  [& opts]
  (riemann.repl/start-server (kwargs-or-map opts)))

(defn service!
  "Ensures that a given service, or its equivalent, is in the next core. If the
  current core includes an equivalent service, uses that service instead.
  Returns the service which will be used in the final core.

  This allows configuration to specify and use services in a way which can,
  where possible, re-use existing services without interruption--e.g., when
  reloading. For example, say you want to use a threadpool executor:

  ```clojure
  (let [executor (service! (ThreadPoolExecutor. 1 2 ...))]
    (where (service \"graphite\")
      (on executor
        graph)))
  ```

  If you reload this config, the *old* executor is busily processing messages
  from the old set of streams. When the new config evaluates (service! ...)
  it creates a new ThreadPoolExecutor and compares it to the existing core's
  services. If it's equivalent, service! will re-use the *existing*
  executor, which prevents having to shut down the old executor.

  But if you *change* the dynamics of the new executor somehow--maybe by
  adjusting a queue depth or max pool size--they won't compare as equivalent.
  When the core transitions, the old executor will be shut down, and the new
  one used to handle any further graphite events.

  Note: Yeah, this does duplicate some of the work done in core/transition!.
  No, I'm not really sure what to do about it. Maybe we need a named service
  registry so all lookups are dynamic. :-/"
  [service]
  (locking core
    (let [service (or (first (filter #(service/equiv? service %)
                                     (:services @core)))
                      service)]
      (swap! next-core core/conj-service service)
      service)))

(defn instrumentation
  "Replaces the default core's instrumentation service with a new one, using
  the given options. If you prefer not to receive any events about Riemann's
  well-being, you can pass :enabled? false.

  (instrumentation {:interval 5
                    :enabled? false})"
  [& opts]
  (let [service (apply core/instrumentation-service opts)]
    (swap! next-core core/conj-service service :force)
    service))

(defn tcp-server
  "Add a new TCP server with opts to the default core.

  (tcp-server {:host \"localhost\" :port 5555})"
  [& opts]
  (service! (tcp/tcp-server (kwargs-or-map opts))))

(defn graphite-server
  "Add a new Graphite TCP server with opts to the default core.

  (graphite-server {:port 2222})"
  [& opts]
  (service! (graphite/graphite-server (kwargs-or-map opts))))

(defn opentsdb-server
  "Add a new OpenTSDB TCP server with opts to the default core.

  (opentsdb-server {:port 4242})"
  [& opts]
  (service! (opentsdb/opentsdb-server (kwargs-or-map opts))))

(defn udp-server
  "Add a new UDP server with opts to the default core.

  (udp-server {:port 5555})"
  [& opts]
  (service! (udp/udp-server (kwargs-or-map opts))))

(defn ws-server
  "Add a new websockets server with opts to the default core.

  (ws-server {:port 5556})"
  [& opts]
  (service! (websockets/ws-server (kwargs-or-map opts))))

(defn sse-server
  "Add a new SSE channel server with opts to the default core.

  (sse-server {:port 5556})"
  [& opts]
  (service! (sse/sse-server (kwargs-or-map opts))))

(defn rabbitmq-transport
  "Add a new RabbitMQ transport to the default core.

  ```
  (rabbitmq-transport {:host \"localhost\"
                       :port 5672
                       :riemann.exchange-settings {:name \"riemann\"
                                                   :auto-delete true}
                       :riemann.routing-key \"riemann.events\"})
  ```
  "
  [& opts]
  (service! (rabbitmq/rabbitmq-transport (kwargs-or-map opts))))

(defn kafka-consumer
  "Add a new kafka consumer with opts to the default core.

  ```
  (kafka-consumer {:consumer.config {:bootstrap.servers \"localhost:9092\"
                                     :group.id \"riemann\"}
                   :topics [\"riemann\"]})
  ```

  Options:

  For a full list of :consumer.config options see the kafka consumer docs.
  NOTE: The :enable.auto.commit option is ignored and defaults to true.

  - :consumer.config      Consumer configuration
      - :bootstrap.servers  Bootstrap configuration, default is \"localhost:9092\"
      - :group.id           Consumer group id, default is \"riemann\"
  - :topics               Topics to consume from, default is [\"riemann\"]
  - :key.deserializer     Key deserializer function, defaults to the
                          keyword-deserializer.
  - :value.deserializer   Value deserializer function, defaults to
                          json-deserializer.
  - :poll.timeout.ms      Polling timeout, default is 100."

  [& opts]
  (service! (kafka/kafka-consumer (kwargs-or-map opts))))

(defn streams
  "Add any number of streams to the default core."
  [& things]
  (locking core
    (swap! next-core assoc :streams
           (reduce conj (:streams @next-core) things))))

(defn index
  "Create a new index and add it to the current core and next core as needed.

  If the current core has no index, add the new index to it.

  To the next core, add the current core's index (when equivalent) or add the
  new index (when not equivalent).

  In practice, because Riemann currently has only one index type (NBHM), only a
  single index will ever be created. It will be preserved across all future
  calls to `index`.

  Returns the index."
  [& opts]
  (locking core
    (let [current-index (:index @core)
          ; Create a new index. Later we'll decide whether it needs to be added
          ; to the current core, next core, or both.
          ; Note that we need to wrap the *current* core's pubsub; the next
          ; core's pubsub module will be discarded in favor of the current one
          ; when core transition takes place.
          new-index (-> (apply riemann.index/index opts)
                        (core/wrap-index (:pubsub @core)))
          ; If the new index is equivalent to the old one, preserve the old one.
          chosen-index (if (service/equiv? current-index new-index)
                         current-index
                         new-index)]
      ; If the current core has no index, add the newly created one.
      (when (nil? current-index) (swap! core assoc :index chosen-index))
      ; Always add the chosen index to the next core, ready for core transition.
      (swap! next-core assoc :index chosen-index)
      chosen-index)))

(defn update-index
  "Updates the given index with all events received. Also publishes to the
  index pubsub channel."
  [index]
  (common/deprecated "(update-index idx) is unnecessary; use idx directly instead. Indexes are now streams themselves, so it's not necessary to wrap them in update-index."
    (fn update [event] (core/update-index @core event))))

(defn delete-from-index
  "Deletes any events that pass through from the index. By default, deletes
  events with the same host and service. If a field, or a list of fields, is
  given, deletes any events with matching values for all of those fields.

  ; Delete all events in the index with the same host
  (delete-from-index :host event)

  ; Delete all events in the index with the same host and state.
  (delete-from-index [:host :state] event)"
  ([]
   (fn delete [event] (core/delete-from-index @core event)))
  ([fields]
   (fn delete [event] (core/delete-from-index @core fields event))))

(defn periodically-expire
  "Sets up a reaper for this core. See riemann.core/reaper."
  ([]
   (periodically-expire 10))
  ([& args]
   (service! (apply core/reaper args))))

(defn reinject
  "A stream which applies any events it receives back into the current core.
  You almost never need this: it makes it easy to create infinite loops, and
  it's rarely the case that you *need* top-level recursion. Where possible,
  prefer a stream that passes events to children.

  (with :metric 1 reinject)"
  [event]
  (core/stream! @core event))

(defn async-queue!
  "A stream which registers (using service!) a new threadpool-service with the
  next core, and returns a stream which accepts events and applies those events
  to child streams via the threadpool service.

  WARNING: this function is not intended for dynamic use. It creates a new
  executor service for *every* invocation. It will not start the executor
  service until the current configuration is applied. Use sparingly and only at
  configuration time--preferably once for each distinct IO-bound asynchronous
  service.

  See `riemann.service/threadpool-service` for options.

  Example:

  ```clojure
  (let [downstream (batch 100 1/10
                          (async-queue! :agg {:queue-size     1e3
                                              :core-pool-size 4
                                              :max-pool-size  32}
                            (forward
                              (riemann.client/tcp-client
                                :host \"127.0.0.1\"))))]
    (streams
      ...
      ; Forward all events downstream to the aggregator.
      (where (service #\"^riemann.*\")
        downstream)))
  ```"
  [name threadpool-service-opts & children]
  (let [s (service! (service/threadpool-service name threadpool-service-opts))]
    (apply execute-on s children)))

(defn publish
  "Returns a stream which publishes events to the given channel. Uses this
  core's pubsub registry."
  [channel]
  (fn [event]
    (pubsub/publish! (:pubsub @core) channel event)))

(defn subscribe
  "Subscribes to the given channel with f, which will receive events. Uses the
  current core's pubsub registry always, because the next core's registry will
  be discarded by core/transition.

  Returns a single-arity function that does nothing with its inputs and, when
  invoked, returns the subscription you created. Why do this weird thing? So
  you can pretend (subscribe ...) is a stream, and use it in the same context
  as your other streams, like (publish)."
  [channel f]
  (let [sub (pubsub/subscribe! (:pubsub @core) channel f)]
    (fn discard [event] sub)))

(defn clear!
  "Resets the next core."
  []
  (locking core
    (reset! next-core (core/core))))

(defn apply!
  "Applies pending changes to the core. Transitions the current core to the
  next one, and resets the next core."
  []
  (locking core
    (swap! core core/transition! @next-core)
    (clear!)))

(defn start!
  "Start the current core."
  []
  (core/start! @core))

(defn stop!
  "Stop the current core."
  []
  (core/stop! @core))

(defn read-strings
  "Returns a sequence of forms read from string."
  ([string]
   (read-strings []
                 (-> string (java.io.StringReader.)
                   (clojure.lang.LineNumberingPushbackReader.))))
  ([forms reader]
   (let [form (clojure.lang.LispReader/read reader false ::EOF false)]
     (if (= ::EOF form)
       forms
       (recur (conj forms form) reader)))))

(def ^:dynamic *config-file*
  "The config file currently being included."
  nil)

(defn config-file-path
  "Computes the full path to a config file. Absolute paths are returned
  unchanged. Relative paths are expanded relative to *config-file*. Returns a
  string."
  [path]
  (if (-> path (file) (.isAbsolute))
    path
    (let [dir (-> (or *config-file* "ZARDOZ")
                (file)
                (.getCanonicalPath)
                (file)
                (.getParent))]
      (str (file dir path)))))

(defn validate-config
  "Check that a config file has valid syntax."
  [file]
  (try
    (read-strings (slurp file))
    (catch clojure.lang.LispReader$ReaderException e
      (throw (logging/nice-syntax-error e file)))))


(defn- config-file?
  "Is the given File a configuration file?"
  [^File file]
  (let [filename (.getName file)]
    (and (.isFile file)
         (or (.matches filename ".*\\.clj$")
             (.matches filename ".*\\.config$")))))

(defn local-repo
  "Sets the location of the local maven repository used
   by `depend` to load plugins"
  [path]
  (reset! riemann.plugin/repo path))

(defmacro depend
  "Pull in specified dependencies. This combines pulling dependencies with
   aether and loading a plugin.

   The option map is fed to com.cemerick.pomegranate/add-dependencies and
   accepts an optional :exit-on-failure keyword defaulting to true which
   indicates whether riemann should bail out when failing to load a plugin
   as well as an option :alias parameter which will be forward to `load-plugin`

   To prefer https, only the clojars repository is registered by default."
  [plugin artifact version options]
  `(let [options# (merge {:coordinates '[[~artifact ~version]]
                          :local-repo (deref riemann.plugin/repo)
                          :exit-on-failure true
                          :repositories {"clojars" "https://clojars.org/repo"}}
                         ~options)]
     (try
       (apply add-dependencies (apply concat options#))
       (load-plugin ~plugin (select-keys options# [:alias]))
       (catch Exception e#
         (error "could not load plugin" (name ~plugin) ":" (.getMessage e#))
         (when (:exit-on-failure options#)
           (System/exit 0))))))

(defn include
  "Include another config file or directory. If the path points to a
   directory, all files with names ending in `.config` or `.clj` within
   it will be loaded recursively.

  ; Relative to the current config file, or cwd
  (include \"foo.clj\")

  ; Absolute path
  (include \"/foo/bar.clj\")"
  [path]
  (let [path (config-file-path path)
        file (file path)]
    (binding [*config-file* path
              *ns* (find-ns 'riemann.config)]
      (if (.isDirectory file)
        (->> file
             file-seq
             (filter config-file?)
             (map str)
             (sort)
             (map include)
             dorun)
        (load-file path)))))
