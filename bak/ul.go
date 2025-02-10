package main

import (
	"bufio"
	"encoding/binary"
	// "encoding/hex"
	"fmt"
	// "io"
	"net"
	"os"
	// "reflect"
	// "strconv"
)

type NodeConfig struct {
	WatchedDirectories []struct {
		Path    string   `yaml:"path"`
		Include []string `yaml:"incl"`
		Exclude []string `yaml:"excl"`
	} `yaml:"dirs"`
}

var FILE_PATH string

// var BUFFER_SIZE uint

func main() {
	// FILE_PATH = os.Args[1]
	FILE_PATH = "./.data/file.1G.dat"
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
		// fmt.Println("Received connection: ", conn, "type(", reflect.TypeOf(conn), ")")
		if err != nil {
			fmt.Println(err)
			continue
		}
		// handle connections in routine
		fmt.Println("New Connection")
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	// net.TCPConn
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
	// defer f.Close()

	// defer conn.Close()
	// fmt.Println("Writing from ", FILE_PATH)
	// // written, err := io.Copy(conn, f)
	// written, err := f.WriteTo(conn)
	//
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	//
	// fmt.Println(written)

	go writeConnStream(conn, stream)
	go readFileStream(f, stream, buffer_size)
}

func flush(writer *bufio.Writer) {
	fmt.Println("flushing writer")
	writer.Flush()
}

func connClose(conn net.Conn) {
	fmt.Println("closing connection")
	conn.Close()
}

func writeConnStream(conn net.Conn, stream <-chan []byte) {
	defer connClose(conn)
	writer := bufio.NewWriter(conn)

	for {
		buf, more := <-stream
		if !more {
			break
		}

		n, err := writer.Write(buf)
		if err != nil {
			fmt.Println(buf, n, err)
			return
		}
		if n < 1 {
			break
		}
	}

	flush(writer)
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
