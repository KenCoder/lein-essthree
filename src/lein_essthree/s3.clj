(ns lein-essthree.s3
  "Thin S3 API wrappers."
  (:require [amazonica.aws.s3 :as s3]
            [amazonica.core :as ac]
            [clojure.java.io :as io]
            [lein-essthree.schemas
             :refer [AWSCreds]]
            [me.raynes.fs :as fs]
            [ring.util.mime-type]
            [pathetic.core :as path]
            [schema.core :as s]))


(s/defn bucket-exists? :- s/Bool
  [aws-creds :- (s/maybe AWSCreds)
   bucket    :- s/Str]
  (ac/with-credential aws-creds
    (s3/does-bucket-exist bucket)))

(s/defn list-objects
  [aws-creds :- (s/maybe AWSCreds)
   bucket    :- s/Str
   path      :- (s/maybe s/Str)]
  (ac/with-credential aws-creds
    (s3/list-objects bucket path)))

(s/defn ^:private path->content-type
  [path  :- s/Str
   auto  :- s/Bool]
  (if auto
    (ring.util.mime-type/ext-mime-type path)
    "application/octet-stream"))

(s/defn put-file!
  [aws-creds :- (s/maybe AWSCreds)
   auto-content-type  :- s/Bool
   bucket             :- s/Str
   obj-key            :- s/Str
   file-path          :- s/Str]
  (ac/with-credential aws-creds
    (with-open [is (io/input-stream file-path)]
      (s3/put-object :bucket-name  bucket
                     :key          obj-key
                     :input-stream is
                     :metadata     {:content-length (fs/size file-path)
                                    :content-type (path->content-type file-path auto-content-type)}))))

(s/defn put-folder!
  [aws-creds   :- (s/maybe AWSCreds)
   bucket      :- s/Str
   folder-path :- s/Str]
  (ac/with-credential aws-creds
    (with-open [is (io/input-stream (byte-array 0))]
      (s3/put-object :bucket-name  bucket
                     :key          (path/ensure-trailing-separator folder-path)
                     :input-stream is
                     :metadata     {:content-length 0}))))

(s/defn delete-object!
  [aws-creds :- (s/maybe AWSCreds)
   bucket    :- s/Str
   obj-key   :- s/Str]
  (ac/with-credential aws-creds
    (s3/delete-object bucket obj-key)))
