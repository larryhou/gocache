package main

import (
    "flag"
    "github.com/larryhou/gocache/server"
    "net/http"
    _ "net/http/pprof"
)

func main() {
    s := server.CacheServer{}
    flag.IntVar(&s.Port,"port", 9966, "server port")
    flag.StringVar(&s.Path, "path", "cache", "cache storage path")
    flag.IntVar(&s.LogLevel, "log-level", 0, "log level debug=-1 info=0 warn=1 error=2 dpanic=3 panic=4 fatal=5")
    flag.IntVar(&s.CacheCap, "cache-cap", 0, "in-memory cache capacity")
    flag.StringVar(&s.Secret, "secret", "larryhou", "connect secret pass")
    flag.BoolVar(&s.UnsafeGet, "unsafe-get", true, "allow getting caches from server even when secret does not match")
    flag.BoolVar(&s.DryRun, "dry-run", false, "dry run mode for profiling, don't use it in practise")
    flag.Parse()

    go http.ListenAndServe(":9999", nil)
    if err := s.Listen(); err != nil { panic(err) }
}
