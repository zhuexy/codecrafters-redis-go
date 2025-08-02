package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Server struct {
	stringData map[string]string
	//hashData map[string]map[string]string
	IP   string
	Port int
	lock sync.Mutex
}

func NewServer(ip string, port int) *Server {
	return &Server{
		stringData: make(map[string]string),
		IP:         ip,
		Port:       port,
	}
}

func (this *Server) Start() {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", this.IP, this.Port))
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
		go this.HandleConn(conn)
	}
}

func (this *Server) HandleConn(conn net.Conn) {
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
		getArgs(reader, args)
		switch args[0] {
		case "PING":
			this.Ping(conn)
		case "ECHO":
			this.Echo(conn, args[1])
		case "SET":
			this.Set(conn, args)
		case "GET":
			this.Get(conn, args)
		}
	}
}

func (this *Server) write(conn net.Conn, msg string) {
	_, err := conn.Write([]byte(msg))
	if err != nil {
		fmt.Println("Failed to write")
	}
}

func (this *Server) Get(conn net.Conn, args []string) {
	this.lock.Lock()
	value, ok := this.stringData[args[1]]
	this.lock.Unlock()
	if ok {
		this.write(conn, "$"+strconv.Itoa(len(value))+"\r\n"+value+"\r\n")
	} else {
		this.write(conn, "$-1\r\n")
	}
}

func (this *Server) Set(conn net.Conn, args []string) {
	if len(args) < 3 {
		fmt.Println("Failed to set")
		return
	}
	this.lock.Lock()
	this.stringData[args[1]] = args[2]
	this.lock.Unlock()
	_, err := conn.Write([]byte("+OK\r\n"))
	if err != nil {
		fmt.Println("Failed to write")
		return
	}
}

func (this *Server) Ping(conn net.Conn) {
	_, err := conn.Write([]byte("+PONG\r\n"))
	if err != nil {
		fmt.Println("Failed to write")
		return
	}
}

func (this *Server) Echo(conn net.Conn, msg string) {
	conn.Write([]byte("+" + msg + "\r\n"))
}

func getArgs(reader *bufio.Reader, args []string) {
	count := cap(args)
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
		// if this arg is command, need to ToUpper
		if i == 0 {
			args[i] = strings.ToUpper(line[:length])
		} else {
			args[i] = line[:length]
		}
	}
}
