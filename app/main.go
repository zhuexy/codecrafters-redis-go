package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
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
	defer conn.Close()
	reader := bufio.NewReader(conn)
	// redis protocol "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Failed to read")
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if !strings.HasPrefix(line, "*") {
			fmt.Println("Failed to parse command")
			return
		}
		count, err := strconv.Atoi(line[1:])
		if err != nil {
			fmt.Println("Failed to parse count")
			return
		}
		args := make([]string, count)
		for i := 0; i < count; i++ {
			line, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Failed to read")
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if !strings.HasPrefix(line, "$") {
				fmt.Println("Failed to parse argument")
				return
			}
			length, err := strconv.Atoi(line[1:])
			if err != nil {
				fmt.Println("Failed to parse length")
				return
			}
			line, err = reader.ReadString('\n')
			if err != nil {
				fmt.Println("Failed to read")
				return
			}
			args[i] = strings.ToUpper(line[:length])
		}
		switch args[0] {
		case "PING":
			ping(conn)
		case "ECHO":
			echo(conn, args[1])
		}

	}
}

func ping(conn net.Conn) {
	conn.Write([]byte("+PONG\r\n"))
}

func echo(conn net.Conn, message string) {
	conn.Write([]byte("+" + message + "\r\n"))
}
