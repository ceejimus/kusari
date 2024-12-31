package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	// "strconv"
)

var FILE_PATH string

// var BUFFER_SIZE uint

func main() {
	FILE_PATH = os.Args[1]
	// buffer_size, _ := strconv.Atoi(os.Args[2])
	// BUFFER_SIZE = uint(buffer_size)

	// TCP listen on port
	ln, err := net.Listen("tcp", ":7337")
	if err != nil {
		fmt.Println(err)
		return
	}
	// accept and handle connections
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		// handle connections in routine
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	buf := make([]byte, 8)
	n, err := conn.Read(buf)
	if n != 8 || err != nil {
		fmt.Println(n, err)
		conn.Close()
		return
	}

	fmt.Println(buf)
	buffer_size := uint(binary.BigEndian.Uint32(buf[0:4]))
	channel_cap := uint(binary.BigEndian.Uint32(buf[4:8]))

	fmt.Printf("Transmitting '%v' with size = %v, cap = %v\n", FILE_PATH, buffer_size, channel_cap)

	stream := make(chan []byte, channel_cap)
	f, err := os.Open(FILE_PATH)
	if err != nil {
		fmt.Println(err)
		return
	}
	go writeConnStream(conn, stream)
	go readFileStream(f, stream, buffer_size)
}

func writeConnStream(conn net.Conn, stream <-chan []byte) {
	defer conn.Close()
	writer := bufio.NewWriter(conn)
	defer writer.Flush()

	for {
		buf, more := <-stream
		if !more {
			break
		}

		n, err := writer.Write(buf)
		// fmt.Println(buf, n)
		if err != nil {
			// fmt.Println(buf, n, err)
			return
		}
		if n < 1 {
			break
		}
	}
}

func readFileStream(f *os.File, stream chan<- []byte, buffer_size uint) {
	defer close(stream)
	defer f.Close()
	r := bufio.NewReader(f)

	for {
		buf := make([]byte, buffer_size)
		n, err := r.Read(buf)
		if err != nil {
			if err.Error() != "EOF" {
				fmt.Println("[ERROR] reading file: ", err)
				return
			}

			stream <- buf[:n]
			break
		}

		stream <- buf[:n]
	}
}
