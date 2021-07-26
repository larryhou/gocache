package main

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/larryhou/gocache/client"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"math"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

var environ struct{
	secret  string
	count   int
	close   float64
	down    float64
	addr    string
	port    int
	cmdPort int

	queue   []*Context
	library []*client.Entity
	cutouts int

	idle chan *Context
	closed chan struct{}
	entreq chan *Context
	entity chan *client.Entity
}

type Context struct {
	work   chan int
	entpsh chan *client.Entity
	u      *client.Engine
}

func (c *Context) Uintptr() uintptr {
	return uintptr(unsafe.Pointer(c))
}

func (c *Context) Close() error {
	close(c.entpsh)
	close(c.work)
	if c.u != nil {return c.u.Close()}
	return nil
}

var logger *zap.Logger

func main() {
	level := 0
	flag.IntVar(&environ.count, "count", 10, "initial client count")
	flag.Float64Var(&environ.close, "close", 0.15, "close ratio[0,1] after upload/download")
	flag.StringVar(&environ.secret, "secret", "larryhou", "command secret")
	flag.Float64Var(&environ.down, "down", 0.95, "download operation ratio[0,1]")
	flag.StringVar(&environ.addr, "addr", "127.0.0.1", "server address")
	flag.IntVar(&environ.port, "port", 9966, "server port")
	flag.IntVar(&environ.cmdPort, "cmd-port", 19966, "local command server port")
	flag.IntVar(&level, "log-level", -1, "log level debug=-1 info=0 warn=1 error=2 dpanic=3 panic=4 fatal=5")
	flag.Parse()

	if v, err := zap.NewDevelopment(zap.IncreaseLevel(zapcore.Level(level))); err != nil {panic(err)} else {logger = v}

	environ.idle = make(chan *Context)
	environ.closed = make(chan struct{})
	environ.entreq = make(chan *Context)
	environ.entity = make(chan *client.Entity)

	go func() {
		for {
			select {
			case ent := <-environ.entity:
				logger.Debug("ENTITY", zap.String("uuid", hex.EncodeToString(ent.Uuid)))
				environ.library = append(environ.library, ent)
			case ctx := <-environ.idle:
				rand.Seed(time.Now().UnixNano())
				num := rand.Int() + 1
				logger.Debug("IDLE", zap.Uintptr("ctx", ctx.Uintptr()), zap.Int("num", num))
				if environ.cutouts > 0 {
					logger.Debug("cut", zap.Uintptr("ctx", ctx.Uintptr()), zap.Int("num", num))
					environ.cutouts--
					ctx.Close()
				} else if float64(num%10000)/10000 > environ.close {
					logger.Debug("assign", zap.Uintptr("ctx", ctx.Uintptr()), zap.Int("num", num))
					ctx.work <- num
				} else {
					ctx.Close()
					go func() {
						environ.closed <- struct{}{} /* notify close event */
						logger.Debug("quit", zap.Uintptr("ctx", ctx.Uintptr()), zap.Float64("close", environ.close), zap.Float64("ratio", 1 - float64(num%100)/100))
					}()
				}
			case ctx := <-environ.entreq:
				logger.Debug("ENTREQ", zap.Uintptr("ctx", ctx.Uintptr()))
				for {
					if len(environ.library) > 0 {
						rand.Seed(time.Now().UnixNano())
						p := rand.Float64()
						span := len(environ.library)
						if span > 200 { span = 200 }
						n := math.Pow(p, 4) * float64(span)
						go func() {
							ctx.entpsh <- environ.library[len(environ.library) - int(n) - 1]
							logger.Debug("send entity", zap.Uintptr("ctx", ctx.Uintptr()))
						}()
						break
					}
					time.Sleep(time.Second)
				}
			case <-environ.closed:
				logger.Debug("CLOSED")
				go addClients(1)
			}
		}
	}()
	go http.ListenAndServe(":9998", nil)
	addClients(environ.count)
	server, err := net.Listen("tcp", fmt.Sprintf(":%d", environ.cmdPort))
	if err != nil { panic(err) }

	for {
		if c, err := server.Accept(); err == nil { go handle(c) }
	}
}

func readString(c net.Conn) (string, error) {
	var buf bytes.Buffer
	b := make([]byte, 1)
	for {
		if _, err := c.Read(b); err != nil {return "", err}
		if b[0] == 0 {return buf.String(),nil}
		buf.WriteByte(b[0])
	}
}

func readInt(c net.Conn) (int, error) {
	num, err := readString(c)
	if err != nil {
		c.Write([]byte(fmt.Sprintf("read int err: %v", err)))
		return 0, err
	}

	num = strings.TrimSpace(num)
	if v, err := strconv.Atoi(num); err != nil {
		c.Write([]byte(fmt.Sprintf("wrong int value: %s", num)))
		return 0, err
	} else { return v, nil }
}

func handle(c net.Conn) {
	defer c.Close()
	if secret, err := readString(c); err != nil {return} else {
		if environ.secret != secret {
			c.Write([]byte(fmt.Sprintf("secret not match: %s", secret)))
			return
		}
	}

	buf := make([]byte, 64)
	if _, err := c.Read(buf[:3]); err != nil {
		c.Write([]byte(fmt.Sprintf("read command err: %v", err)))
		return
	}

	for {
		cmd := string(buf[:3])
		logger.Info("command", zap.String("name", cmd), zap.String("addr", c.RemoteAddr().String()))
		switch cmd {
		case "add":
			if num, err := readInt(c); err != nil {return} else {go addClients(num)}
		case "cut":
			if num, err := readInt(c); err != nil {return} else {go cutClients(num)}
		case "clo":
			if num, err := readInt(c); err != nil {return} else {
				if num > 100 { num = 100 } else if num < 0 { num = 0 }
				environ.close = float64(num) / 100
			}
		case "dow":
			if num, err := readInt(c); err != nil {return} else {
				if num > 100 { num = 100 } else if num < 0 { num = 0 }
				environ.down = float64(num) / 100
			}
		default: return
		}
	}
}

func addClients(num int) {
	for i := 0; i < num; i++ {
		u := &client.Engine{Addr: environ.addr, Port: environ.port}
		if err := u.Connect(); err != nil {
			u.Close()
			go func() {
				environ.closed <- struct{}{}
				logger.Error("connect err", zap.Error(err))
			}()
			continue
		}
		go runClient(u)
	}
}

func runClient(u *client.Engine) {
	defer u.Close()
	ctx := &Context{u: u, work: make(chan int), entpsh: make(chan *client.Entity)}
	logger.Debug("client", zap.Uintptr("ctx", ctx.Uintptr()))
	go func() {
		environ.idle <- ctx
		logger.Debug("push idle n", zap.Uintptr("ctx", ctx.Uintptr()))
	}()

	for {
		select {
		case num := <-ctx.work:
			if num == 0 {return}
			logger.Debug("++++", zap.Uintptr("ctx", ctx.Uintptr()), zap.Int("num", num))
			if float64(num%10000)/10000 > environ.down || len(environ.library) == 0 {
				logger.Debug("upload", zap.Uintptr("ctx", ctx.Uintptr()))
				if ent, err := u.Upload(); err == nil {
					logger.Debug("upload", zap.Uintptr("ctx", ctx.Uintptr()), zap.String("uuid", hex.EncodeToString(ent.Uuid)))
					go func() {
						environ.idle <- ctx
						logger.Debug("push idle u", zap.Uintptr("ctx", ctx.Uintptr()))
					}()
					go func() {
						environ.entity <- ent
						logger.Debug("push entity", zap.Uintptr("ctx", ctx.Uintptr()))
					}()
				} else {
					logger.Error("upload err: %v", zap.Error(err))
				}
			} else {
				go func() {
					environ.entreq <- ctx /* send download request */
					logger.Debug("down req")
				}()
			}
		case ent := <-ctx.entpsh:
			if ent == nil {return}
			logger.Debug("down", zap.Uintptr("ctx", ctx.Uintptr()), zap.String("uuid", hex.EncodeToString(ent.Uuid)))
			if err := u.Download(ent); err != nil {
				logger.Error("down err: %v",
					zap.String("uuid", hex.EncodeToString(ent.Uuid)), zap.Error(err))
			} else {
				go func() {
					environ.idle <- ctx
					logger.Debug("push idle d", zap.Uintptr("ctx", ctx.Uintptr()))
				}()
			}
		}
	}
}

func cutClients(num int) {
	if num > 0 { environ.cutouts = num }
}
