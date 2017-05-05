(defproject onyx-reducers "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :profiles {:dev {:dependencies []
                   :resource-paths ["test-resources/"]}
             :circle-ci {:global-vars {*warn-on-reflection* true}
                         :jvm-opts ["-Xmx2500M"
                                    "-XX:+UnlockCommercialFeatures"
                                    "-XX:+FlightRecorder"
                                    "-XX:StartFlightRecording=duration=1080s,filename=recording.jfr"]}
             :clojure-1.7 {:dependencies [[org.clojure/clojure "1.7.0"]]}
             :clojure-1.8 {:dependencies [[org.clojure/clojure "1.8.0"]]}}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.onyxplatform/onyx "0.10.0-SNAPSHOT"]
                 [com.clearspring.analytics/stream "2.7.0"]])
