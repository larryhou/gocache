package main

import (
    "flag"
    "github.com/larryhou/gocache/client"
    "math/rand"
    "time"
)

func main() {
    s := rand.NewSource(time.Now().UnixNano())
    r := rand.New(s)

    down := 0.95
    c := &client.Engine{Rand: r}
    flag.BoolVar(&c.Verify, "verify", true, "verify sha256")
    flag.StringVar(&c.Addr, "addr", "127.0.0.1", "server address")
    flag.IntVar(&c.Port, "port", 9966, "server port")
    flag.Float64Var(&down, "down", 0.95, "download operation ratio")
    flag.Parse()

    var cache *client.Entity
    if err := c.Connect(); err != nil {panic(err)} else {
        for {
            if r.Float64() <= down && cache != nil {
                if err := c.Download(cache); err != nil {panic(err)}
            } else {
                if entity, err := c.Upload(); err != nil {panic(err)} else { cache = entity }
            }
        }
    }
}
