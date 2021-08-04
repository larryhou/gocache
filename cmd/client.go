package main

import (
    "encoding/hex"
    "flag"
    "fmt"
    "github.com/larryhou/gocache/client"
    "math/rand"
    "os"
    "path"
    "strconv"
    "time"
)

func main() {
    s := rand.NewSource(time.Now().UnixNano())
    r := rand.New(s)

    var vars struct{command,path,output string; uuid string; t int; days uint}

    c := &client.Engine{Rand: r}
    flag.StringVar(&vars.command, "command", "get", "supported commands: get | put | uget | uput | clean")
    flag.StringVar(&vars.path, "path", "", "resource path")
    flag.StringVar(&vars.uuid, "uuid", "", "resource entity uuid")
    flag.StringVar(&c.Addr, "addr", "127.0.0.1", "server address")
    flag.IntVar(&c.Port, "port", 9966, "server port")
    flag.IntVar(&vars.t, "type", 0, "resource type")
    flag.StringVar(&vars.output, "output", ".", "get output directory")
    flag.StringVar(&c.Version, "version", "cliv2.0", "cache version")
    flag.UintVar(&vars.days, "days", 30, "number of days, used with clean command")
    flag.Parse()

    if err := c.Connect(); err != nil {panic(err)}

    uuid := make([]byte, 32)
    if _, err := hex.Decode(uuid, []byte(vars.uuid)); err != nil {
        if len(vars.uuid) > len(uuid) { copy(uuid, vars.uuid[:32]) } else { copy(uuid, vars.uuid) }
    }

    filename := path.Join(vars.output, hex.EncodeToString(uuid)+"."+strconv.Itoa(vars.t))

    switch vars.command {
    case "get":
        if _, err := os.Stat(vars.output); err != nil && os.IsNotExist(err) { os.MkdirAll(vars.output, 0700) }
        if file, err := os.OpenFile(filename, os.O_CREATE | os.O_WRONLY, 0700); err == nil {
            defer file.Close()
            if err := c.Get(uuid, vars.t, file); err != nil {panic(err)}
        } else {panic(err)}
    case "put":
        if file, err := os.Open(vars.path); err == nil {
            defer file.Close()
            if s, err := file.Stat(); err == nil {
                if err := c.Put(uuid, vars.t, s.Size(), file); err != nil {panic(err)}
            } else {panic(err)}
        } else {panic(err)}
    case "uget":
        if _, err := os.Stat(vars.output); err != nil && os.IsNotExist(err) { os.MkdirAll(vars.output, 0700) }
        if file, err := os.OpenFile(filename, os.O_CREATE | os.O_WRONLY, 0700); err == nil {
            defer file.Close()
            if err := c.UGet(uuid, vars.t, vars.path, file); err != nil {panic(err)}
        } else {panic(err)}
    case "uput":
        if err := c.UPut(uuid, vars.t, vars.path); err != nil {panic(err)}
    case "clean": c.Clean(uint16(vars.days))
    default: panic(fmt.Sprintf("unknown command: %s", vars.command))
    }
}
