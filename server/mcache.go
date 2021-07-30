package server

import (
    "bytes"
    "errors"
    "fmt"
    "go.uber.org/zap"
    "io"
    "os"
    "sync"
    "time"
    "unsafe"
)

type File struct {
    uuid string
    name string
    size int64
    m    *bytes.Buffer
    f    *os.File
    w    io.Writer
    r    io.Reader
    c    bool
}

func (f *File) Read(p []byte) (int, error) {
    if f.r == nil { if f.f != nil {f.r = f.f} else {f.r = f.m} }
    return f.r.Read(p)
}

func (f *File) Write(p []byte) (int, error) {
    if f.w == nil {
        var w []io.Writer
        if f.m != nil { w = append(w, f.m) }
        if f.f != nil { w = append(w, f.f) }
        f.w = io.MultiWriter(w...)
    }
    return f.w.Write(p)
}

func (f *File) Close() error {
    defer func() {
        if err := f.tryCache(); err == mcache.errors.cacherr {
            f.m.Reset()
            logger.Debug("pool", zap.Uintptr("put", uintptr(unsafe.Pointer(f.m))))
            f.m = nil
        }
    }()
    if f.f != nil { return f.f.Close() }
    return nil
}

func (f *File) tryCache() error {
    if f.m != nil && f.f != nil {
        if s, err := os.Stat(f.name); err == nil {
            if s.Size() == int64(f.m.Len()) {
                mcache.core.put(f.uuid, f.m)
                return nil
            }
        }
        return mcache.errors.cacherr
    }
    return mcache.errors.unavailable
}

func (f *File) Name() string { return f.name }

type memEntity struct {
    data *bytes.Buffer
    uuid string
    size int64
    ts   int64
    hit  int
}

type memCache struct {
    capacity int
    lookups  map[string]*memEntity
    library  []*memEntity
    size     int64
    g        int
    p        int
    n        int
    sync.RWMutex
}

func (m *memCache) remove(uuid string) {
    if entity, ok := m.lookups[uuid]; ok {
        delete(m.lookups, entity.uuid)
        m.size -= int64(entity.data.Cap())
        for i := 0; i < len(m.library); i++ {
            e := m.library[i]
            if e.uuid == uuid {
                m.library = append(m.library[:i], m.library[i+1:]...)
                break
            }
        }
    }
}

func (m *memCache) put(uuid string, data *bytes.Buffer) {
    m.Lock()
    defer m.Unlock()
    m.p++
    logger.Debug("mcache", zap.String("put", uuid), zap.Int("size", data.Len()), zap.Uintptr("ptr", uintptr(unsafe.Pointer(data))))
    m.remove(uuid) /* clean up old one */
    entity := &memEntity{uuid: uuid, data: data, size: int64(data.Len()), ts: time.Now().UnixNano()}
    m.lookups[uuid] = entity
    m.library = append(m.library, entity)
    m.size += int64(data.Cap())
    if m.capacity < len(m.library) {
        for i := 0; i < len(m.library); i++ {
            entity := m.library[i]
            if m.capacity < len(m.library) {
                logger.Debug("mcache cls", zap.Int("cap", m.capacity), zap.Int("len", len(m.library)))
                delete(m.lookups, entity.uuid)
                m.library = append(m.library[:i], m.library[i+1:]...)
                m.size -= int64(entity.data.Cap())
                i--
            } else { break }
        }
    }
}

func (m *memCache) stat() {
    for {
        m.RLock()
        var size int64
        for i := 0; i < len(m.library); i++ {
            entity := m.library[i]
            size += int64(entity.data.Cap())
        }
        m.RUnlock()
        logger.Debug("mcache", zap.Int("library", len(m.library)),
            zap.Int("lookups", len(m.lookups)),
            zap.Int64("size", size),
            zap.Int("get", mcache.core.g),
            zap.Float64("gpt", float64(mcache.core.g) / float64(mcache.core.n)),
            zap.Int("put", mcache.core.p))
        time.Sleep(5 * time.Second)
    }
}

func (m *memCache) get(uuid string) (*bytes.Buffer, error) {
    m.RLock()
    defer m.RUnlock()
    m.n++
    if entity, ok := m.lookups[uuid]; ok {
        entity.hit++
        m.g++
        logger.Debug("mcache", zap.String("get", uuid),
            zap.Uintptr("ptr", uintptr(unsafe.Pointer(entity.data))),
            zap.Int("size", entity.data.Len()),
            zap.Int("data", int(entity.size)),
            zap.Int("cap", entity.data.Cap()))
        return bytes.NewBuffer(entity.data.Bytes()), nil
    }
    return nil, mcache.errors.unavailable
}

var mcache struct {
    core   memCache
    limit  int64
    errors struct {
        unavailable error
        cacherr     error
    }
}

func init() {
    mcache.limit = 2 << 20 // 2M
    mcache.errors.unavailable = errors.New("not available for caching")
    mcache.errors.cacherr = errors.New("cache error")
    mcache.core.lookups = make(map[string]*memEntity)
}

func Open(name string, uuid string) (*File, error) {
    if mcache.core.capacity > 0 {
        if data, err := mcache.core.get(uuid); err == nil {
            return &File{m: data, uuid: uuid, size: int64(data.Len()), c: true}, nil
        }
    }
    file, err := os.Open(name)
    if err != nil {return nil, err}
    f := &File{f: file, name: name, uuid: uuid}
    if s, err := file.Stat(); err == nil {
        f.size = s.Size()
        if mcache.core.capacity > 0 && s.Size() < mcache.limit {
            f.m = bytes.NewBuffer(make([]byte, 0, s.Size()))
        }
    } else {
        file.Close()
        return nil, fmt.Errorf("stat err: %s", name)
    }
    return f, nil
}

func NewFile(name string, uuid string, size int64) (*File, error) {
    file, err := os.OpenFile(name, os.O_CREATE | os.O_WRONLY, 0700)
    if err != nil {return nil, err}
    f := &File{f: file, name: name, uuid: uuid}
    if mcache.core.capacity > 0 && size < mcache.limit {
        f.m = bytes.NewBuffer(make([]byte, 0, size))
    }
    return f, nil
}
