package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Failed to read")
			return
		}
		message := strings.TrimSpace(string(buf[:n]))
		fmt.Println("Received: ", message)
		if message == "PING" {
			_, err = conn.Write([]byte("+PONG\r\n"))
			if err != nil {
				fmt.Println("Failed to write")
				return
			}
		} else {
			words := strings.Split(message, " ")
			if strings.ToLower(words[0]) == "echo" {
				_, err = conn.Write([]byte("+" + words[1] + "\r\n"))
				if err != nil {
					fmt.Println("Failed to write")
					return
				}
			}
		}
	}
}
