package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/andrei-cloud/anet/broker"
)

func main() {
	fmt.Println("listening on :3456")
	l, err := net.Listen("tcp", ":3456")
	if err != nil {
		panic(err)
	}
	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			panic(err)
		}

		fmt.Printf("connection form: %s\n", c.RemoteAddr())

		go handler(c)
	}
}

func handler(c net.Conn) {
	defer c.Close()
	for {
		start := time.Now()
		msg, err := broker.Decode(bufio.NewReader(c))
		if err != nil {
			if err == io.EOF {
				log.Printf("connection %s is closed\n", c.RemoteAddr())
				return
			}
			log.Println(err)
			return
		}
		log.Printf("%s -> %s\n", c.RemoteAddr(), string(msg))

		msg = append(msg, []byte("_response")...)
		out, err := broker.Encode(msg)
		if err != nil {
			log.Println(err)
			return
		}
		n, err := c.Write(out)
		if err != nil {
			log.Println(err)
			return
		}
		log.Printf("write %d bytes to %s, in %v\n", n, c.RemoteAddr(), time.Since(start))
	}
}
