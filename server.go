package anet

import (
	"bufio"
	"io"
	"log"
	"net"
	"time"

	"context"
)

func spinTestServer() (string, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	l, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		panic(err)
	}

	go func() {
		defer l.Close()
		select {
		case <-ctx.Done():
			return
		default:
			for {
				c, err := l.Accept()
				if err != nil {
					panic(err)
				}

				go handler(ctx, c)
			}
		}
	}()

	return l.Addr().String(), cancel
}

func handler(ctx context.Context, c net.Conn) {
	defer c.Close()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			start := time.Now()
			msg, err := Decode(bufio.NewReader(c))
			if err != nil {
				if err != io.EOF {
					log.Println(err)
				}
				return
			}
			log.Printf("%s -> %s\n", c.RemoteAddr(), string(msg))

			msg = append(msg, []byte("_response")...)
			out, err := Encode(msg)
			if err != nil {
				if err != io.EOF {
					log.Println(err)
				}
				return
			}
			n, err := c.Write(out)
			if err != nil {
				if err != io.EOF {
					log.Println(err)
				}
				return
			}
			log.Printf("write %d bytes to %s, in %v\n", n, c.RemoteAddr(), time.Since(start))
		}
	}
}
