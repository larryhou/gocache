package main

import (
    "flag"
    "fmt"
    "github.com/larryhou/gocache/server"
    "log"
    "net"
    "strconv"
    "time"
)

var config struct {
    port int
    addr string
    bufs int
}

func main() {
    flag.StringVar(&config.addr, "addr", "127.0.0.1", "empty value implicit it's server")
    flag.IntVar(&config.port, "port", 12345, "server port")
    flag.IntVar(&config.bufs, "buf-size", 64, "send and receive buffer size/KB")
    flag.Parse()

    if len(config.addr) == 0 {
        if listener, err := net.Listen("tcp", ":" + strconv.Itoa(config.port)); err != nil {panic(err)} else {
            for {
                if c, err :=listener.Accept(); err == nil {
                    go handleServer(c)
                }
            }
        }
    } else {
        if c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", config.addr, config.port)); err != nil {panic(err)} else {
            handleClient(c)
        }
    }
}

func handleServer(c net.Conn) {
    f := true
    defer func() {
        c.Close()
        f = false
        log.Printf("%s closed", c.RemoteAddr().String())
    }()
    log.Printf("%s connected", c.RemoteAddr().String())

    buf := make([]byte, config.bufs<<10)
    s := &server.Stream{Rwp: c}
    n := int64(0)
    t := time.Now()

    go func() {
        for f {
            elapse := time.Now().Sub(t).Seconds()
            speed := float64(n) / elapse / (1<<30)
            log.Printf("%s %.2fG\n", c.RemoteAddr().String(), speed)
            time.Sleep(5 * time.Second)
        }
    }()

    for {
        if err := s.Read(buf, len(buf)); err != nil {return}
        n += int64(len(buf))
    }
}

func handleClient(c net.Conn) {
    defer c.Close()
    buf := make([]byte, config.bufs<<10)
    s := &server.Stream{Rwp: c}

    n := int64(0)
    t := time.Now()
    go func() {
        for {
            elapse := time.Now().Sub(t).Seconds()
            speed := float64(n) / elapse / (1<<30)
            log.Printf("%s %.2fG\n", c.RemoteAddr().String(), speed)
            time.Sleep(time.Second)
        }
    }()

    for {
        if err := s.Write(buf, len(buf)); err != nil {panic(err)}
        n += int64(len(buf))
    }
}
