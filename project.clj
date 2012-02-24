(defn dirname [path]
    (.getParent (java.io.File. path)))

(defn expand-path [path]
    (.getCanonicalPath (java.io.File. path)))

(defn relative-path [path]
    (expand-path (str (dirname *file*) "/" path)))

(defproject riemann "0.0.3-SNAPSHOT"
  :description 
"A network event stream processor. Intended for analytics, metrics, and
alerting; and to glue various monitoring systems together."
  :url "http://github.com/aphyr/riemann"
  :repositories {
    "boundary-site" "http://maven.boundary.com/artifactory/repo"
  }
  :dependencies [
                 [clojure "1.3.0"]
                 [org.clojure/math.numeric-tower "0.0.1"]
                 [org.clojure/algo.generic "0.1.0"]
    [org.clojure/tools.logging "0.2.3"]
    [log4j/log4j "1.2.16" :exclusions [javax.mail/mail
                                       javax.jms/jms
                                       com.sun.jdmk/jmxtools
                                       com.sun.jmx/jmxri]]
    [log4j/apache-log4j-extras "1.0"]
    [aleph "0.2.1-alpha2-SNAPSHOT"]
    [io.netty/netty "3.3.0.Final"]
    [protobuf "0.6.0-beta7"]
    [org.antlr/antlr "3.2"]
    [com.boundary/high-scale-lib "1.0.3"]
    [clj-time "0.3.4"]
    [com.draines/postal "1.7-SNAPSHOT"]
  ]
  :dev-dependencies [
    [lein-deb "1.0.0-SNAPSHOT"]
    [protobuf "0.6.0-beta7"]
    [lein-autodoc "0.9.0"]
    [codox "0.4.0"]
  ]
  :test-selectors {:default (complement :integration)
                   :integration :integration
                   :all (fn [_] true)}
  :java-source-path "src/riemann/"
  :aot [riemann.bin]
  :main riemann.bin
  ; Work around a bug where protobufs get nuked.
  :disable-implicit-clean true
  :deb {:maintainer {:name "Kyle Kingsbury"
                     :email "aphyr@aphyr.com"}
        ; I wish I could use relative paths here, but lein-deb complains
        ; "No directory specified for tarfileset", and lein macros need them
        ; to be strings. Arrrgh.
        :filesets [{:file     "/home/aphyr/riemann/riemann-0.0.3-SNAPSHOT-standalone.jar"
                    :fullpath "/usr/lib/riemann/riemann.jar"}
                   {:file     "/home/aphyr/riemann/riemann.config"
                    :fullpath "/etc/riemann/riemann.config"}
                   {:file     "/home/aphyr/riemann/bin/riemann"
                    :fullpath "/usr/bin/riemann"
                    :filemode "0755"}]
        :depends ""}
  :deb-skip-jar true
  :codox {:output-dir "site/api"}
)
