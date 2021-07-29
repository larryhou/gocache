package server

import (
    "bytes"
    "encoding/binary"
    "encoding/hex"
    "fmt"
    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
    "io"
    "math/rand"
    "net"
    "os"
    "path"
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
        if i, err := s.Rwp.Read(p[t:n]); err != nil {return err} else {t+=i}
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
    dsize := int64(0)
    ts := time.Now()
    defer func() {
        c.Close()
        if dsize > 0 {
            elapse := time.Now().Sub(ts).Seconds()
            speed := float64(dsize) / elapse
            logger.Info("closed w", zap.String("addr", addr), zap.Int64("size", dsize), zap.Float64("speed", speed), zap.Float64("elapse", elapse))
        } else { logger.Info("closed w", zap.String("addr", addr)) }
    }()
    conn := &Stream{Rwp: c}

    buf := make([]byte, 64<<10)
    for ctx := range event {
        switch ctx.command {
        case 'g':
            t := strconv.Itoa(ctx.t)
            size := int64(0)
            exists := true
            filename := path.Join(s.Path, version, ctx.uuid[:2], ctx.uuid, t)
            if s.DryRun { size = 2 << 20 } else {
                logger.Debug("get +++", zap.String("uuid", ctx.uuid), zap.Int("type", ctx.t))
                fi, err := os.Stat(filename)
                exists = err == nil || os.IsExist(err)
                if exists { size = fi.Size() }
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
            dsize += int64(p)
            if !exists {continue}

            logger.Debug("get >>>", zap.String("uuid", ctx.uuid), zap.Int("type", ctx.t), zap.Int64("size", size))

            var in *Stream
            if s.DryRun { in = &Stream{Rwp: Air{}} } else {
                file, err := Open(filename, ctx.uuid+t)
                if err != nil {logger.Error("get read cache err", zap.String("file", filename), zap.Error(err));return }
                in = &Stream{Rwp: file}
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
            if sent == size { logger.Debug("get success", zap.Int64("sent", sent), zap.String("file", filename)) }
            dsize += sent
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
    usize := int64(0)
    defer func() {
        if ready {close(event)} else {c.Close()}
        if usize > 0 {
            elapse := time.Now().Sub(ts).Seconds()
            speed := float64(usize) / elapse
            logger.Info("closed r", zap.String("addr", addr), zap.Int64("size", usize), zap.Float64("speed", speed), zap.Float64("elapse", elapse))
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
        usize++
        cmd := buf[0]
        if err := conn.Read(buf,32+4); err != nil { logger.Error("read get id err", zap.Error(err));return }
        id := buf[:32]
        uuid := hex.EncodeToString(id)
        t := int(binary.BigEndian.Uint32(buf[32:]))
        b := buf[36:]
        usize += 36

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
            usize += 8
            size := int64(binary.BigEndian.Uint64(b))
            logger.Debug("put", zap.String("uuid", uuid), zap.Int("type", t), zap.Int64("size", size))
            if !safe { /* read and discard in unsafe mode */
                write := int64(0)
                for write < size {
                    num := int64(len(buf))
                    if size - write < num { num = size - write }
                    if err := conn.Read(buf, int(num)); err != nil {return} else {write += num}
                }
                usize += write
                logger.Debug("put discard", zap.String("uuid", uuid), zap.Int("type", t), zap.Int64("size", write))
                continue
            }

            var out *Stream
            t := strconv.Itoa(t)
            dir := path.Join(s.Path, version, uuid[:2], uuid)
            filename := path.Join(dir, t)
            if s.DryRun {out = &Stream{Rwp: Air{}}} else {
                if _, err := os.Stat(dir); err != nil && os.IsNotExist(err) { os.MkdirAll(dir, 0700) }
                name := buf[:32]
                rand.Read(name)
                if _, err := os.Stat(s.temp); err != nil && os.IsNotExist(err) { os.MkdirAll(s.temp, 0700) }
                file, err := NewFile(path.Join(s.temp, hex.EncodeToString(name)), uuid+t, size)
                if err != nil {logger.Error("put create file err", zap.String("file", filename), zap.Error(err));return}
                out = &Stream{Rwp: file}
            }

            write := int64(0)
            for write < size {
                num := int64(len(buf))
                if size - write < num { num = size - write }
                if err := conn.Read(buf, int(num)); err != nil {out.Close();os.Remove(out.Name());return} else {
                    write += num
                    if err := out.Write(buf, int(num)); err != nil {
                        out.Close()
                        os.Remove(out.Name())
                        logger.Error("put write cache err", zap.Int64("write", write), zap.Int64("size", size), zap.Error(err))
                        return
                    }
                }
            }
            out.Close()
            if !s.DryRun {
                if err := os.Rename(out.Name(), filename); err != nil {
                    logger.Error("put failure", zap.String("type", t), zap.Int64("write", write), zap.String("file", filename), zap.Error(err))
                    return
                }
            }
            logger.Debug("put success", zap.String("type", t), zap.Int64("write", write), zap.String("file", filename))
            usize += write
        default:
            logger.Error("unsupported command", zap.String("cmd", string(cmd)))
            return
        }
    }
}