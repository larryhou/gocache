package client

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/larryhou/gocache/server"
	"hash"
	"io"
	rand2 "math/rand"
	"net"
)

type Engine struct {
	Addr    string
	Port    int
	Verify  bool
	Rand    *rand2.Rand
	Version string
	c       *server.Stream
	b       [32 << 10]byte
}

func (e *Engine) Close() error {
	if e.c != nil {
		return e.c.Close()
	}
	return nil
}

func (e *Engine) Connect() error {
	c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", e.Addr, e.Port))
	if err != nil {return err}
	e.c = &server.Stream{Rwp: c}
	buf := e.b[:]
	secret := "larryhou"
	if err := e.c.WriteString(buf, secret); err != nil {return err}
	if err := e.c.WriteString(buf, e.Version); err != nil {return err}
	if ver, err := e.c.ReadString(buf); err != nil {return err} else {
		if ver != e.Version {return fmt.Errorf("version not match: %s != %s", ver, e.Version)}
	}
	return nil
}

func (e *Engine) UGet(id []byte, t int, u string, w io.Writer) error {
	p := 0
	b := e.b[0:]
	b[p] = 'u'
	p++
	copy(b[p:], id)
	p += len(id)
	binary.BigEndian.PutUint32(b[p:], uint32(t))
	p += 4
	b[p] = 'g'
	p++
	binary.BigEndian.PutUint16(b[p:], uint16(len(u)))
	p += 2
	copy(b[p:], u)
	p += len(u)
	if err := e.c.Write(b, p); err != nil {return err}
	return e.get(id, t, w)
}

func (e *Engine) UPut(id []byte, t int, u string) error {
	p := 0
	b := e.b[0:]
	b[p] = 'u'
	p++
	copy(b[p:], id)
	p += len(id)
	binary.BigEndian.PutUint32(b[p:], uint32(t))
	p += 4
	b[p] = 'p'
	p++
	binary.BigEndian.PutUint16(b[p:], uint16(len(u)))
	p += 2
	copy(b[p:], u)
	p += len(u)
	return e.c.Write(b, p)
}

func (e *Engine) Get(id []byte, t int, w io.Writer) error {
	p := 0
	b := e.b[0:]
	b[p] = 'g'
	p++
	copy(b[p:], id)
	p += len(id)
	binary.BigEndian.PutUint32(b[p:], uint32(t))
	p += 4

	if err := e.c.Write(b, p); err != nil {return err}
	return e.get(id, t, w)
}

func (e *Engine) get(id []byte, t int, w io.Writer) error {
	b := e.b[:]
	n := 1 + 32 + 4 + 8
	if err := e.c.Read(b, n); err != nil {return err}
	c := b[0]
	b = b[1:]
	if !bytes.Equal(b[:32], id) {return fmt.Errorf("get id not match: %s != %s",
		hex.EncodeToString(b[:32]), hex.EncodeToString(id))}
	b = b[32:]
	rt := int(binary.BigEndian.Uint32(b))
	if rt != t {return fmt.Errorf("get type not match: %d != %d", rt, t)}
	b = b[4:]
	size := int64(binary.BigEndian.Uint64(b))
	if c == '-' {return nil}
	if c != '+' {return fmt.Errorf("get cmd not match: %c != +", c)}

	read := int64(0)
	for read < size {
		num := int64(len(e.b))
		if size - read < num { num = size - read }
		b = e.b[:num]
		if err := e.c.Read(b, int(num)); err != nil {return fmt.Errorf("read:%c %d != %d err: %v", t, read, size, err)} else {
			read += num
			for b := b; len(b) > 0; {
				if m, err := w.Write(b); err != nil {return err} else { b = b[m:] }
			}
		}
	}
	return nil
}

func (e *Engine) Put(id []byte, t int, size int64, r io.Reader) error {
	p := 0
	b := e.b[:]
	b[p] = 'p'
	p++
	copy(b[p:], id)
	p += len(id)
	binary.BigEndian.PutUint32(b[p:], uint32(t))
	p += 4
	binary.BigEndian.PutUint64(b[p:], uint64(size))
	p += 8
	if err := e.c.Write(b, p); err != nil {return err}
	sent := int64(0)
	for sent < size {
		num := int64(len(e.b))
		if size - sent < num { num = size - sent }
		b := e.b[:num]
		if n, err := r.Read(b); err != nil {return err} else {
			if err := e.c.Write(b, n); err != nil {return err}
			sent += int64(n)
		}
	}
	return nil
}

func (e *Engine) Clean(days uint16) error {
	p := 0
	b := e.b[:]
	b[p] = 'c'
	p++
	binary.BigEndian.PutUint16(b[p:], days)
	p += 2
	return e.c.Write(b, p)
}

func (e *Engine) Pump(size int64, w io.Writer) error {
	buf := make([]byte, 64<<10)
	sent := int64(0)
	for sent < size {
		num := int64(len(buf))
		if size - sent < num { num = size - sent }
		b := buf[:num]
		rand.Read(b[:64])
		sent += num
		for len(b) > 0 {
			if n, err := w.Write(b); err != nil {return err} else {b = b[n:]}
		}
	}
	return nil
}

type Entity struct {
	Uuid []byte
	Sha0 []byte
	Sha1 []byte
	Size int64
}

func (e *Engine) Upload() (*Entity, error) {
	ent := &Entity{}
	ent.Uuid = make([]byte, 32)
	rand.Read(ent.Uuid)

	size := (16<<10) + int64(e.Rand.Intn(2<<20))
	ent.Size = size
	{
		r, w := io.Pipe()
		go func() {
			defer w.Close()
			h := sha256.New()
			f := io.MultiWriter(w, h)
			if err := e.Pump(size, f); err != nil {return}
			ent.Sha0 = h.Sum(nil)
		}()

		e.Put(ent.Uuid, 0, size, r)
	}
	if e.Rand.Int() % 3 > 0 {
		r, w := io.Pipe()
		size := size / 10
		go func() {
			defer w.Close()
			h := sha256.New()
			f := io.MultiWriter(w, h)
			if err := e.Pump(size, f); err != nil {return}
			ent.Sha1 = h.Sum(nil)
		}()

		e.Put(ent.Uuid, 1, size, r)
	}

	return ent, nil
}

type Counter int64
func (c *Counter) Write(p []byte) (int, error) {
	*c += Counter(len(p))
	return len(p), nil
}

func (e *Engine) Download(ent *Entity) error {
	{
		var c Counter
		var h hash.Hash
		var w io.Writer
		if !e.Verify {w = &c} else {
			h = sha256.New()
			w = io.MultiWriter(&c, h)
		}
		if err := e.Get(ent.Uuid, 0, w); err != nil {return err}
		if h != nil {
			if c == 0 { return nil }
			if int64(c) != ent.Size {return fmt.Errorf("size not match: %d != %d", c, ent.Size)}
			s := h.Sum(nil)
			if len(ent.Sha0) > 0 && !bytes.Equal(s, ent.Sha0[:32]) {panic(fmt.Errorf("sha0 not match: %s != %s %s %d", hex.EncodeToString(s), hex.EncodeToString(ent.Sha0), hex.EncodeToString(ent.Uuid), c))}
		}
	}
	if len(ent.Sha1) > 0 {
		var c Counter
		var h hash.Hash
		var w io.Writer
		if !e.Verify {w = &c} else {
			h = sha256.New()
			w = io.MultiWriter(&c, h)
		}
		if err := e.Get(ent.Uuid, 1, w); err != nil {return err}
		if h != nil {
			if c == 0 {return nil}
			s := h.Sum(nil)
			if len(ent.Sha1) > 0 && !bytes.Equal(s, ent.Sha1[:32]) {panic(fmt.Errorf("sha1 not match: %s != %s %s %d", hex.EncodeToString(s), hex.EncodeToString(ent.Sha1), hex.EncodeToString(ent.Uuid), c))}
		}
	}
	return nil
}