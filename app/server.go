package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Server struct {
	strData  map[string]StrData
	listData map[string]ListData
	IP       string
	Port     int
	lock     sync.Mutex
}

func NewServer(ip string, port int) *Server {
	return &Server{
		strData:  make(map[string]StrData),
		listData: make(map[string]ListData),
		IP:       ip,
		Port:     port,
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
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			fmt.Println("Failed to close conn")
		}
	}(conn)
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
		command := strings.ToUpper(args[0])
		switch command {
		case "PING":
			this.Ping(conn)
		case "ECHO":
			this.Echo(conn, args[1])
		case "SET":
			this.Set(conn, args)
		case "GET":
			this.Get(conn, args)
		case "RPUSH":
			this.RPush(conn, args)
		case "LRANGE":
			this.LRange(conn, args)
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
	key := args[1]
	this.lock.Lock()
	data, ok := this.strData[key]
	this.lock.Unlock()
	if !ok {
		this.write(conn, "$-1\r\n")
		return
	}
	if data.Expire == -1 || time.Now().UnixMilli() < data.Expire {
		this.write(conn, "$"+strconv.Itoa(len(data.Value))+"\r\n"+data.Value+"\r\n")
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
	data := StrData{
		Value:  args[2],
		Expire: -1,
	}
	if len(args) > 3 && strings.ToLower(args[3]) == "px" {
		if len(args) < 5 {
			fmt.Println("Failed to set")
			return
		}
		expire, err := strconv.Atoi(args[4])
		if err != nil {
			fmt.Println("Failed to parse expire")
			this.lock.Unlock()
			return
		}
		data.Expire = time.Now().UnixMilli() + int64(expire)
	}
	this.strData[args[1]] = data
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
	_, err := conn.Write([]byte("+" + msg + "\r\n"))
	if err != nil {
		fmt.Println("Failed to write")
	}
}

func (this *Server) RPush(conn net.Conn, args []string) {
	if len(args) < 3 {
		fmt.Println("Failed to rpush")
		return
	}
	key := args[1]
	this.lock.Lock()
	data, ok := this.listData[key]
	if !ok {
		data = ListData{
			Value:  make([]string, 0),
			Expire: -1,
		}
	}
	for i := 2; i < len(args); i++ {
		data.Value = append(data.Value, args[i])
	}
	this.listData[key] = data
	this.lock.Unlock()
	this.write(conn, ":"+strconv.Itoa(len(data.Value))+"\r\n")
}

func (this *Server) LRange(conn net.Conn, args []string) {
	if len(args) < 4 {
		fmt.Println("Failed to lrange")
		return
	}
	key := args[1]
	start, err := strconv.Atoi(args[2])
	if err != nil {
		fmt.Println("Failed to parse start")
		return
	}
	stop, err := strconv.Atoi(args[3])
	if err != nil {
		fmt.Println("Failed to parse stop")
		return
	}
	if start > stop {
		this.write(conn, "*0\r\n")
		return
	}
	this.lock.Lock()
	data, ok := this.listData[key]
	this.lock.Unlock()
	if !ok || start > len(data.Value)-1 {
		this.write(conn, "*0\r\n")
		return
	}
	if stop > len(data.Value) {
		stop = len(data.Value) - 1
	}
	result := data.Value[start : stop+1]
	this.writeList(conn, result)
}

func (this *Server) writeList(conn net.Conn, result []string) {
	this.write(conn, "*"+strconv.Itoa(len(result))+"\r\n")
	for _, value := range result {
		this.write(conn, "$"+strconv.Itoa(len(value))+"\r\n"+value+"\r\n")
	}
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
		args[i] = line[:length]
	}
}
