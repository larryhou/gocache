package server

import (
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

type CacheServer struct {
    Port     int
    Path     string
    LogLevel int
    CacheCap int
    temp     string
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
    out := &Stream{Rwp: c}

    buf := make([]byte, 1280)
    for ctx := range event {
        switch ctx.command {
        case 'g':
            t := strconv.Itoa(ctx.t)
            filename := path.Join(s.Path, version, ctx.uuid[:2], ctx.uuid, t)
            logger.Debug("get +++", zap.String("uuid", ctx.uuid), zap.Int("type", ctx.t))
            fi, err := os.Stat(filename)
            exist := false
            p := 0
            if err != nil && os.IsNotExist(err) {
                buf[p] = '-'
                p++
                logger.Debug("mis ---", zap.String("uuid", ctx.uuid), zap.Int("type", ctx.t))
            } else {
                exist = true
                buf[p] = '+'
                p++
            }
            copy(buf[p:], ctx.id[:])
            p += len(ctx.id)
            binary.BigEndian.PutUint32(buf[p:], uint32(ctx.t))
            p += 4
            if exist {
                binary.BigEndian.PutUint64(buf[p:], uint64(fi.Size()))
            } else {
                binary.BigEndian.PutUint64(buf[p:], 0)
            }
            p += 8

            if err := out.Write(buf, p); err != nil { logger.Error("send get + err", zap.Error(err));return }
            dsize += int64(p)
            if !exist {continue}

            logger.Debug("get >>>", zap.String("uuid", ctx.uuid), zap.Int("type", ctx.t), zap.Int64("size", fi.Size()))

            file, err := Open(filename, ctx.uuid+t)
            if err != nil {logger.Error("get read cache err", zap.String("file", filename), zap.Error(err));return }
            in := &Stream{Rwp: file}
            sent := int64(0)
            for size := fi.Size(); sent < size; {
                num := int64(len(buf))
                if size - sent < num { num = size - sent }
                if err := in.Read(buf, int(num)); err != nil {
                    in.Close()
                    logger.Error("get read file err", zap.Int64("sent", sent), zap.Int64("size", size), zap.Error(err))
                    return
                } else {
                    sent += num
                    if err := out.Write(buf, int(num)); err != nil {
                        in.Close()
                        logger.Error("get sent body err", zap.Int64("sent", sent), zap.Int64("size", size), zap.Error(err))
                        return
                    }
                    //logger.Debug("get", zap.Int64("size", size), zap.Int64("sent", sent))
                }
            }
            in.Close()
            if sent == fi.Size() { logger.Debug("get success", zap.Int64("sent", sent), zap.String("file", filename)) }
            dsize += sent
        }
    }
}

func (s *CacheServer) Handle(c net.Conn) {
    conn := &Stream{Rwp: c}
    addr := c.RemoteAddr().String()
    logger.Info("connected", zap.String("addr", addr))
    event := make(chan *Context)
    ts := time.Now()
    usize := int64(0)
    defer func() {
        close(event)
        if usize > 0 {
            elapse := time.Now().Sub(ts).Seconds()
            speed := float64(usize) / elapse
            logger.Info("closed r", zap.String("addr", addr), zap.Int64("size", usize), zap.Float64("speed", speed), zap.Float64("elapse", elapse))
        } else { logger.Info("closed r", zap.String("addr", addr)) }
    }()

    var version string
    buf := make([]byte, 1024)
    {
        if err := conn.Read(buf, 2); err != nil {logger.Error("read version size err", zap.Error(err));return}
        n := int(binary.BigEndian.Uint16(buf))
        if err := conn.Read(buf[2:], n); err != nil {logger.Error("read version err", zap.Error(err));return}
        version = string(buf[2:2+n])
        if err := conn.Write(buf, n+2); err != nil {logger.Error("echo version err", zap.String("ver", version), zap.Error(err));return}
        logger.Debug("handshake", zap.String("ver", version))
        usize += 2 + int64(n)
    }

    go s.Send(c, event, version)
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

            t := strconv.Itoa(t)
            dir := path.Join(s.Path, version, uuid[:2], uuid)
            if _, err := os.Stat(dir); err != nil && os.IsNotExist(err) { os.MkdirAll(dir, 0700) }
            filename := path.Join(dir, t)

            name := buf[:32]
            rand.Read(name)
            if _, err := os.Stat(s.temp); err != nil && os.IsNotExist(err) { os.MkdirAll(s.temp, 0700) }
            file, err := NewFile(path.Join(s.temp, hex.EncodeToString(name)), uuid+t, size)
            if err != nil {logger.Error("put create file err", zap.String("file", filename), zap.Error(err));return}
            out := &Stream{Rwp: file}
            write := int64(0)
            for write < size {
                num := int64(len(buf))
                if size - write < num { num = size - write }
                if err := conn.Read(buf, int(num)); err != nil {out.Close();os.Remove(file.Name());return} else {
                    write += num
                    if err := out.Write(buf, int(num)); err != nil {
                        out.Close()
                        os.Remove(file.Name())
                        logger.Error("put write cache err", zap.Int64("write", write), zap.Int64("size", size), zap.Error(err))
                        return
                    }
                    //logger.Debug("put", zap.Int64("size", size), zap.Int64("write", write))
                }
            }
            out.Close()
            if err := os.Rename(file.Name(), filename); err != nil {
                logger.Error("put failure", zap.String("type", t), zap.Int64("write", write), zap.String("file", filename), zap.Error(err))
                return
            }
            if write == size { logger.Debug("put success", zap.String("type", t), zap.Int64("write", write), zap.String("file", filename))} else {
                logger.Error("put", zap.Int64("size", size), zap.Int64("write", write))
            }
            usize += write
        default:
            logger.Error("unsupported command", zap.String("cmd", string(cmd)))
            return
        }
    }
}