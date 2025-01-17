// package main
//
// import (
// 	// "bufio"
// 	"encoding/binary"
// 	// // "encoding/hex"
// 	"fmt"
// 	"io"
// 	"net"
// 	"os"
// 	// // "strconv"
// )
//
// func writeDlParams(conn net.Conn, buf_size uint32, chan_cap uint32) {
// 	outbuf := make([]byte, 8)
// 	binary.BigEndian.PutUint32(outbuf[0:4], buf_size)
// 	binary.BigEndian.PutUint32(outbuf[4:8], chan_cap)
// 	conn.Write(outbuf)
// }
//
// func main() {
// 	conn, err := net.Dial("tcp", "0.0.0.0:7337")
// 	if err != nil {
// 		fmt.Println(err)
// 		panic(err)
// 	}
//
// 	defer conn.Close()
//
// 	writeDlParams(conn, uint32(8192*8), uint32(8))
//
// 	path := "./.data/test.out"
// 	f, err := os.Create(path)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer f.Close()
//
// 	total, err := io.Copy(f, conn)
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	fmt.Printf("%v bytes downloaded\n", total)
// }
