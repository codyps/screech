(defproject jepsen.screech "0.1.0-SNAPSHOT"
  :description "Screech durability under Jepsen and LazyFS faults; adapted from jepsen-io/nats"
  :license {:name "EPL-2.0" :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.3"]
                 [jepsen "0.3.10"]
                 [io.nats/jnats "2.24.1"]
                 [cheshire "5.13.0"]]
  :repositories [["central" {:url "https://repo.maven.apache.org/maven2/"}]
                 ["clojars" {:url "https://repo.clojars.org/"}]]
  :local-repo "../../results/jepsen-runtime/m2"
  :jvm-opts ["-Xmx2g" "-Djava.awt.headless=true"]
  :main jepsen.screech.cli)
