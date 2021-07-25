package client

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/larryhou/gocache/server"
	"io"
	rand2 "math/rand"
	"net"
	"time"
)

type Unity struct {
	Addr string
	Port int
	c    net.Conn
	b    [1024]byte
}

func (u *Unity) Close() error {
	if u.c != nil {
		return u.c.Close()
	}
	return nil
}

func (u *Unity) Connect() error {
	c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", u.Addr, u.Port))
	if err != nil {return err}
	u.c = c
	conn := &server.Stream{Rwp: c}
	buf := u.b[:]
	secret := "larryhou"
	if err := conn.WriteString(buf, secret); err != nil {return err}
	version := "simv2.0"
	if err := conn.WriteString(buf, version); err != nil {return err}
	if ver, err := conn.ReadString(buf); err != nil {return err} else {
		if ver != version {return fmt.Errorf("version not match: %s != %s", ver, version)}
	}
	return nil
}

func (u *Unity) Get(id []byte, t int, w io.Writer) error {
	p := 0
	b := u.b[0:]
	b[p] = 'g'
	p++
	copy(b[p:], id)
	p += len(id)
	binary.BigEndian.PutUint32(b[p:], uint32(t))
	p += 4

	conn := &server.Stream{Rwp: u.c}
	if err := conn.Write(b, p); err != nil {return err}

	n := 1 + 32 + 4 + 8
	if err := conn.Read(b, n); err != nil {return err}
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
		num := int64(len(u.b))
		if size - read < num { num = size - read }
		b = u.b[:num]
		if err := conn.Read(b, int(num)); err != nil {return fmt.Errorf("read:%c %d != %d err: %v", t, read, size, err)} else {
			read += num
			for b := b; len(b) > 0; {
				if m, err := w.Write(b); err != nil {return err} else { b = b[m:] }
			}
		}
	}
	return nil
}

func (u *Unity) Put(id []byte, t int, size int64, r io.Reader) error {
	p := 0
	b := u.b[:]
	b[p] = 'p'
	p++
	copy(b[p:], id)
	p += len(id)
	binary.BigEndian.PutUint32(b[p:], uint32(t))
	p += 4
	binary.BigEndian.PutUint64(b[p:], uint64(size))
	p += 8
	conn := &server.Stream{Rwp: u.c}
	if err := conn.Write(b, p); err != nil {return err}
	sent := int64(0)
	for sent < size {
		num := int64(len(u.b))
		if size - sent < num { num = size - sent }
		b := u.b[:num]
		if n, err := r.Read(b); err != nil {return err} else {
			if err := conn.Write(b, n); err != nil {return err}
			sent += int64(n)
		}
	}
	return nil
}

func (u *Unity) Pump(size int64, w io.Writer) error {
	buf := make([]byte, 1280)
	sent := int64(0)
	for sent < size {
		num := int64(len(buf))
		if size - sent < num { num = size - sent }
		b := buf[:num]
		rand.Read(b)
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

func (u *Unity) Upload() (*Entity, error) {
	ent := &Entity{}
	ent.Uuid = make([]byte, 32)
	rand.Read(ent.Uuid)

	rand2.Seed(time.Now().UnixNano())
	size := (16<<10) + int64(rand2.Intn(2<<20))
	ent.Size = size
	{
		r, w := io.Pipe()
		go func() {
			defer w.Close()
			h := sha256.New()
			f := io.MultiWriter(w, h)
			if err := u.Pump(size, f); err != nil {return}
			ent.Sha0 = h.Sum(nil)
		}()

		u.Put(ent.Uuid, 0, size, r)
		fmt.Printf("upload success size=%d sha=%s\n", size, hex.EncodeToString(ent.Sha0))
	}
	if rand2.Int() % 3 > 0 {
		r, w := io.Pipe()
		size := size / 10
		go func() {
			defer w.Close()
			h := sha256.New()
			f := io.MultiWriter(w, h)
			if err := u.Pump(size, f); err != nil {return}
			ent.Sha1 = h.Sum(nil)
		}()

		u.Put(ent.Uuid, 1, size, r)
	}

	return ent, nil
}

type Counter int64
func (c *Counter) Write(p []byte) (int, error) {
	*c += Counter(len(p))
	return len(p), nil
}

func (u *Unity) Download(ent *Entity) error {
	{
		var c Counter
		h := sha256.New()
		w := io.MultiWriter(&c, h)
		if err := u.Get(ent.Uuid, 0, w); err != nil {return err}
		if c == 0 { return nil }
		if int64(c) != ent.Size {return fmt.Errorf("size not match: %d != %d", c, ent.Size)}
		s := h.Sum(nil)
		if len(ent.Sha0) > 0 && !bytes.Equal(s, ent.Sha0[:32]) {panic(fmt.Errorf("sha0 not match: %s != %s %s %d", hex.EncodeToString(s), hex.EncodeToString(ent.Sha0), hex.EncodeToString(ent.Uuid), c))}
	}
	if len(ent.Sha1) > 0 {
		var c Counter
		h := sha256.New()
		w := io.MultiWriter(&c, h)
		if err := u.Get(ent.Uuid, 1, w); err != nil {return err}
		if c == 0 {return nil}
		s := h.Sum(nil)
		if len(ent.Sha1) > 0 && !bytes.Equal(s, ent.Sha1[:32]) {panic(fmt.Errorf("sha1 not match: %s != %s %s %d", hex.EncodeToString(s), hex.EncodeToString(ent.Sha1), hex.EncodeToString(ent.Uuid), c))}
	}
	return nil
}