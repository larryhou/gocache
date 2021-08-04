package server

import (
    "bytes"
    "encoding/binary"
    "encoding/hex"
    "fmt"
    "github.com/djherbis/times"
    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
    "io"
    "math/rand"
    "net"
    "net/http"
    "os"
    "path"
    "path/filepath"
    "strconv"
    "time"
)

var logger *zap.Logger

type Entity struct {
    uuid string
    id   [32]byte
    t    int
}

type Context struct {
    Entity
    command byte
    file *WebFile
}

type WebFile struct {
    body io.ReadCloser
    size int64
    file *os.File
    name string
}

func (f *WebFile) Read(p []byte) (int, error) {
    n, err := f.body.Read(p)
    t := 0
    for t < n { if i, err := f.file.Write(p[t:n]); err != nil {return n, err} else {t+=i} }
    return n, err
}

func (f *WebFile) Write(p []byte) (int, error) {
    return len(p), nil
}

func (f *WebFile) Close() error {
    defer os.Rename(f.file.Name(), f.name)
    defer f.file.Close()
    logger.Debug("close web file")
    return f.body.Close()
}

type Stream struct {
    Rwp io.ReadWriter
}

func (s *Stream) Name() string {
    if f, ok := s.Rwp.(*File); ok {return f.Name()}
    return ""
}

func (s *Stream) ReadString(buf []byte) (string, error) {
    if err := s.Read(buf, 2); err != nil {return "", err}
    n := int(binary.BigEndian.Uint16(buf))
    if n < cap(buf) {
        if err := s.Read(buf, n); err != nil {return "", err}
        return string(buf[:n]), nil
    } else {
        b := &bytes.Buffer{}
        for t := 0; t < n; {
            num := cap(buf)
            if n - t < num {num = n - t}
            if err := s.Read(buf, num); err != nil {return "", err}
            if _, err := b.Write(buf[:num]); err != nil {return "", err}
            t += num
        }
        return b.String(), nil
    }
}

func (s *Stream) WriteString(buf []byte, v string) error {
    n := len(v)
    binary.BigEndian.PutUint16(buf, uint16(n))
    if err := s.Write(buf, 2); err != nil {return err}
    for t := 0; t < n; {
        num := cap(buf)
        if n - t < num {num = n - t}
        copy(buf, v[t:t+num])
        if err := s.Write(buf, num); err != nil {return err}
        t += num
    }
    return nil
}

func (s *Stream) Read(p []byte, n int) error {
    for t := 0; t < n; {
        if i, err := s.Rwp.Read(p[t:n]); err == nil {t+=i} else {
            if err == io.EOF && t + i == n {return nil}
            return err
        }
    }
    return nil
}

func (s *Stream) Write(p []byte, n int) error {
    for t := 0; t < n; {
        if i, err := s.Rwp.Write(p[t:n]); err != nil {return err} else {t+=i}
    }
    return nil
}

func (s *Stream) Close() error {
    if c, ok := s.Rwp.(io.Closer); ok { return c.Close() }
    return nil
}

type Air struct { }
func (i Air) Read(p []byte) (int, error)  { return len(p), nil }
func (i Air) Write(p []byte) (int, error) { return len(p), nil }

type CacheServer struct {
    Port      int
    Path      string
    LogLevel  int
    CacheCap  int
    Secret    string
    UnsafeGet bool
    DryRun    bool
    temp      string
    cleants   time.Time
}

func (s *CacheServer) Listen() error {
    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
    if err != nil {return err}
    mcache.core.capacity = s.CacheCap
    s.temp = path.Join(s.Path, "temp")
    {
        l, err := zap.NewDevelopment(zap.IncreaseLevel(zapcore.Level(s.LogLevel)))
        if err != nil { panic(err) }
        logger = l
    }
    //go mcache.core.stat()
    for {
        c, err := listener.Accept()
        if err != nil { continue }
        go s.Handle(c)
    }
}

func (s *CacheServer) Send(c net.Conn, event chan *Context, version string) {
    addr := c.RemoteAddr().String()
    outgoing := int64(0)
    ts := time.Now()
    defer func() {
        c.Close()
        if outgoing > 0 {
            elapse := time.Now().Sub(ts).Seconds()
            speed := float64(outgoing) / elapse
            logger.Info("closed w", zap.String("addr", addr), zap.Int64("size", outgoing), zap.Float64("speed", speed), zap.Float64("elapse", elapse))
        } else { logger.Info("closed w", zap.String("addr", addr)) }
    }()
    conn := &Stream{Rwp: c}

    buf := make([]byte, 64<<10)
    for ctx := range event {
        switch ctx.command {
        case 'g':
            t := strconv.Itoa(ctx.t)

            exists := true
            var in *Stream
            size := int64(0)
            filename := path.Join(s.Path, version, ctx.uuid[:2], ctx.uuid, t)
            if s.DryRun {
                in = &Stream{Rwp: &Air{}}
                size = 2<<20
            } else {
                if ctx.file != nil {
                    in = &Stream{Rwp: ctx.file}
                    size = ctx.file.size
                } else {
                    file, err := Open(filename, ctx.uuid+t)
                    if err == nil { size = file.size } else { exists = false }
                    in = &Stream{Rwp: file}
                }
            }

            p := 0
            if !exists {
                buf[p] = '-'
                p++
                logger.Debug("mis ---", zap.String("uuid", ctx.uuid), zap.Int("type", ctx.t))
            } else {
                exists = true
                buf[p] = '+'
                p++
            }
            copy(buf[p:], ctx.id[:])
            p += len(ctx.id)
            binary.BigEndian.PutUint32(buf[p:], uint32(ctx.t))
            p += 4
            if exists {
                binary.BigEndian.PutUint64(buf[p:], uint64(size))
            } else {
                binary.BigEndian.PutUint64(buf[p:], 0)
            }
            p += 8

            if err := conn.Write(buf, p); err != nil { logger.Error("send get + err", zap.Error(err));return }
            outgoing += int64(p)
            if !exists {continue}

            logger.Debug("get >>>", zap.String("uuid", ctx.uuid), zap.Int("type", ctx.t), zap.Int64("size", size))
            if file, ok := in.Rwp.(*File); ok && file.c {
                m := file.m
                if err := conn.Write(m.Bytes(), m.Len()); err != nil {
                    logger.Error("get sent cache err", zap.Int64("size", size), zap.Error(err))
                    return
                }
                outgoing += int64(m.Len())
                logger.Debug("get success", zap.String("type", t), zap.Int("sent", m.Len()), zap.String("file", filename), zap.Bool("cache", true))
                continue
            }

            sent := int64(0)
            for sent < size {
                num := int64(len(buf))
                if size - sent < num { num = size - sent }
                if err := in.Read(buf, int(num)); err != nil {
                    in.Close()
                    logger.Error("get read file err", zap.Int64("sent", sent), zap.Int64("size", size), zap.Error(err))
                    return
                } else {
                    sent += num
                    if err := conn.Write(buf, int(num)); err != nil {
                        in.Close()
                        logger.Error("get sent body err", zap.Int64("sent", sent), zap.Int64("size", size), zap.Error(err))
                        return
                    }
                }
            }
            in.Close()
            logger.Debug("get success", zap.Int64("sent", sent), zap.String("file", filename))
            outgoing += sent
        }
    }
}

func (s *CacheServer) Handle(c net.Conn) {
    ready := false
    conn := &Stream{Rwp: c}
    addr := c.RemoteAddr().String()
    logger.Info("connected", zap.String("addr", addr))
    event := make(chan *Context)
    ts := time.Now()
    incoming := int64(0)
    defer func() {
        if ready {close(event)} else {c.Close()}
        if incoming > 0 {
            elapse := time.Now().Sub(ts).Seconds()
            speed := float64(incoming) / elapse
            logger.Info("closed r", zap.String("addr", addr), zap.Int64("size", incoming), zap.Float64("speed", speed), zap.Float64("elapse", elapse))
        } else { logger.Info("closed r", zap.String("addr", addr)) }
    }()

    safe := true
    var version string
    buf := make([]byte, 16<<10)
    if secret, err := conn.ReadString(buf); err != nil {return} else {
        if secret != s.Secret {
            if !s.UnsafeGet{return}
            safe = false
        }
    }

    if ver, err := conn.ReadString(buf); err == nil {version = ver} else {
        logger.Error("read version err", zap.Error(err));return
    }
    if err := conn.WriteString(buf, version); err != nil {
        logger.Error("echo version err", zap.String("ver", version), zap.Error(err));return
    }

    logger.Debug("handshake", zap.String("addr", addr), zap.String("ver", version))
    go s.Send(c, event, version)
    ready = true

    for {
        if err := conn.Read(buf, 1); err != nil {
            if err != io.EOF { logger.Error("read command err", zap.Error(err)) }
            return
        }
        incoming++
        cmd := buf[0]
        switch cmd {
        case 'c':
            if conn.Read(buf, 2) == nil { go s.clean(version, binary.BigEndian.Uint16(buf)) } else {return}
            incoming += 2
            continue
        }

        if err := conn.Read(buf,32+4); err != nil { logger.Error("read get id err", zap.Error(err));return }
        id := buf[:32]
        uuid := hex.EncodeToString(id)
        t := int(binary.BigEndian.Uint32(buf[32:]))
        b := buf[36:]
        incoming += 36

        switch cmd {
        case 'g':
            ctx := &Context{}
            ctx.command = cmd
            ctx.uuid = uuid
            ctx.t = t
            copy(ctx.id[:], id)
            logger.Debug("get", zap.String("uuid", ctx.uuid))
            event <- ctx

        case 'p':
            if err := conn.Read(b, 8); err != nil {logger.Error("put read id err", zap.Error(err));return}
            incoming += 8
            size := int64(binary.BigEndian.Uint64(b))
            logger.Debug("put", zap.String("uuid", uuid), zap.Int("type", t), zap.Int64("size", size))

            var out *Stream
            t := strconv.Itoa(t)
            dir := path.Join(s.Path, version, uuid[:2], uuid)
            filename := path.Join(dir, t)
            if s.DryRun || !safe {out = &Stream{Rwp: Air{}}} else {
                if err := s.mkdir(dir); err != nil {return}
                name := buf[:32]
                rand.Read(name)
                if err := s.mktemp(); err != nil {return}
                file, err := NewFile(path.Join(s.temp, hex.EncodeToString(name)), uuid+t, size)
                if err != nil {logger.Error("put init err", zap.String("file", filename), zap.Error(err));return}
                out = &Stream{Rwp: file}
            }

            received := int64(0)
            for received < size {
                num := int64(len(buf))
                if size - received < num { num = size - received }
                if err := conn.Read(buf, int(num)); err != nil {out.Close();os.Remove(out.Name());return} else {
                    received += num
                    if err := out.Write(buf, int(num)); err != nil {
                        out.Close()
                        os.Remove(out.Name())
                        logger.Error("put save err", zap.String("type", t), zap.Int64("received", received), zap.Int64("size", size), zap.Error(err))
                        return
                    }
                }
            }
            out.Close()
            if _, ok := out.Rwp.(*File); ok {
                if err := os.Rename(out.Name(), filename); err != nil {
                    logger.Error("put failure", zap.String("type", t), zap.Int64("received", received), zap.String("file", filename), zap.Error(err))
                    return
                }
            }
            logger.Debug("put success", zap.String("type", t), zap.Int64("received", received), zap.String("file", filename))
            incoming += received
        case 'u':
            if err := conn.Read(b, 1); err != nil {return}
            cmd := b[0]

            u := ""
            if s, err := conn.ReadString(b); err == nil {u=s} else {logger.Error("url", zap.Error(err));return}
            dir := path.Join(s.Path, version, uuid[:2], uuid)
            filename := path.Join(dir, strconv.Itoa(t))
            switch cmd {
            case 'g':
                ctx := &Context{}
                ctx.command = cmd
                ctx.uuid = uuid
                ctx.t = t
                copy(ctx.id[:], id)
                logger.Debug("uget", zap.String("url", u))
                if _, err := os.Stat(filename); err != nil && os.IsNotExist(err) {
                    if rsp, err := http.Get(u); err == nil {
                        success := false
                        if rsp.ContentLength > 0 {
                            if err := s.mktemp(); err == nil {
                                rand.Read(buf[:32])
                                if f, err := os.OpenFile(path.Join(s.temp, hex.EncodeToString(buf[:32])), os.O_CREATE | os.O_WRONLY, 0700); err == nil {
                                    if err := s.mkdir(dir); err == nil {
                                        success = true
                                        ctx.file = &WebFile{body: rsp.Body, size: rsp.ContentLength, name: filename, file: f}
                                        logger.Debug("uget pipe", zap.Int64("size", rsp.ContentLength), zap.String("url", u))
                                    } else {
                                        logger.Error("uget pipe", zap.Int64("size", rsp.ContentLength), zap.String("url", u), zap.Error(err))
                                    }
                                }
                            }
                        }
                        if !success { rsp.Body.Close() }
                    }
                }
                event <- ctx
            case 'p':
                logger.Debug("uput", zap.String("url", u))
                if rsp, err := http.Get(u); err == nil {
                    go func() {
                        defer rsp.Body.Close()
                        if rsp.ContentLength > 0 {
                            if err := s.mktemp(); err == nil {
                                name := make([]byte, 32)
                                rand.Read(name)
                                if f, err := os.OpenFile(path.Join(s.temp, hex.EncodeToString(name)), os.O_CREATE | os.O_WRONLY, 0700); err == nil {
                                    if n, err := io.Copy(f, rsp.Body); err != nil {
                                        logger.Error("uput", zap.Int64("received", n), zap.Int64("expect", rsp.ContentLength), zap.String("url", u), zap.Error(err))
                                        f.Close()
                                        return
                                    }
                                    if s.mkdir(dir) == nil {
                                        f.Close()
                                        if err := os.Rename(f.Name(), filename); err == nil {
                                            logger.Debug("uput success", zap.Int64("size", rsp.ContentLength), zap.String("url", u), zap.String("name", filename))
                                        } else {
                                            logger.Error("uput failure", zap.Int64("size", rsp.ContentLength), zap.String("url", u), zap.Error(err))
                                        }
                                    }
                                }
                            }
                        }
                    }()
                } else { logger.Error("uput", zap.String("url", u), zap.Error(err)) }
            }
        default:
            logger.Error("unsupported command", zap.String("cmd", string(cmd)))
            return
        }
    }
}

func (s *CacheServer) mktemp() error {
    if _, err := os.Stat(s.temp); err != nil && os.IsNotExist(err) { return os.MkdirAll(s.temp, 0700) }
    return nil
}

func (s *CacheServer) mkdir(dir string) error {
    if _, err := os.Stat(dir); err != nil && os.IsNotExist(err) { return os.MkdirAll(dir, 0700) }
    return nil
}

func (s *CacheServer) clean(version string, days uint16) {
    ts := time.Now()
    if ts.Sub(s.cleants) < time.Hour {return}
    s.cleants = ts

    dir := path.Join(s.Path, version)
    logger.Info("clean", zap.String("dir", dir), zap.Uint16("days", days))
    filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
        if err != nil {return err}
        if t := times.Get(info); !info.IsDir() {
            if !t.AccessTime().IsZero() && ts.Sub(t.AccessTime()) > time.Duration(days) * 24 * time.Hour {
                if err := os.Remove(path); err == nil {
                    logger.Info("clean", zap.String("name", path), zap.Int64("size", info.Size()))
                }
            }
        }
        return nil
    })
}