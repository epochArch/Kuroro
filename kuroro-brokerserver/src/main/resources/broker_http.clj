(ns http
  (:import (com.epocharch.kuroro.common Constants))
  (:use ring.adapter.jetty)
  (:use [clojure.core.match :only [match]]))

(defn handler [request]
      {:status 200
       :headers {"Content-Type" "text/json"}
       :body (let [uri (:uri request) query-string (:query-string request)]
                  (println (str uri "," query-string))
                  (match [uri]
                         ["/ok"] "OK"
                         [_] (str "API not supported" "(" uri "?" query-string ")")))})

(defn start [ip port]
  (println (str "Starting http server listening on" " " ip ":" port))
  (run-jetty handler {:port port :join? false}))

(defn start_leader_http []
  (start "127.0.0.1" Constants/KURORO_HTTP_PORT))