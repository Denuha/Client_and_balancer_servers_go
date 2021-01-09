package main

import (
	"encoding/json"
	"fmt"
	"gopkg.in/ini.v1"
	"net"
	"os"
	"strconv"
	"time"
	//"math"
	m "./mess"
)

const N = 3 // Количетсво серверов
var Servers [N]Server

const MaxInt = 9223372036854775807

type Server struct {
	Port      string
	Queue     chan m.Message
	ProcSpeed int // Скорость сервера
	Name      string
	server_id int
}

func (srv *Server) Listen() error {
	port := srv.Port
	if port == "" {
		port = ":4545"
	}

	listener, err := net.Listen("tcp", port)

	if err != nil {
		fmt.Println(srv.Name, "listener error", err)
		return err
	}
	defer listener.Close()
	go srv.PrintCountQueue()

	// только сервер-балансировщик проверяет свою очередь для распределения нагрузки
	if srv.server_id == 0 {
		go srv.CheckQueue()
	}
	go srv.ParseQueue()

	fmt.Println(srv.Name, "is listening...")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			conn.Close()
			continue
		}
		go srv.handleConnection(conn) // запускаем горутину для обработки запроса
	}
}

func (srv *Server) Balance() {
	ind_nbs := -1 // Индекс самого свободного сервера без (балансировщик не учитвается)
	tmp_q := MaxInt

	for i := 1; i < N; i++ { // Балансировщик не учитывается, поэтому индекс с 1
		//for i, s := range Servers{ // Ищем свободный сервер
		if Servers[i].CountQueue() < tmp_q {
			tmp_q = Servers[i].CountQueue()
			ind_nbs = i
		}
	}
	srv_dest := Servers[ind_nbs] // сервер-получатель

	sum := 0 // общая мощность серверов
	for _, s := range Servers {
		sum += s.ProcSpeed
	}

	q_bal := srv.CountQueue()                  // / sum) // очередь на балансировщике (server1)
	n := int(q_bal / sum * srv_dest.ProcSpeed) // Количество сообщений для серверака от балансировщика
	//n := srv_dest.ProcSpeed
	if n < 1 {
		n = srv_dest.ProcSpeed
	}

	// если очередь на выбранном сервере меньше, чем на балансировщике,
	// то перекидывавем на него пакеты
	if srv_dest.CountQueue() <= srv.CountQueue() {
		srv.SendMessage(srv_dest, n)
	}
}

func (srv *Server) CheckQueue() {
	// проверка, если выростает очередь на балансировщике, то происходит распределение нагрузки
	for {
		// если выросла очередь на балансировщике в 2 раза, то начинаем распределение нагрузки
		if srv.CountQueue() > srv.ProcSpeed*2 {
			srv.Balance()
		}
	}
}

func (srv *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		// считываем полученные в запросе данные
		input := make([]byte, (50 * 1024 * 4))
		n, err := conn.Read(input)
		if n == 0 || err != nil {
			//fmt.Println(srv.Name, "Read error:", err)
			break
		}

		count_mess := srv.Input2Queue(input, n)
		fmt.Println(srv.Name, "Получено структур", count_mess)
	}
}

func (srv *Server) Input2Queue(input []byte, n int) int {
	// Массив Json переводится в очередь chan Message

	var messages []m.Message
	err := json.Unmarshal(input[0:n], &messages)
	if err != nil {
		fmt.Println("error in Input2Queue:", err)
	}
	count_mess := 0
	for _, m := range messages {
		srv.Queue <- m
		count_mess++
	}
	return count_mess
}

func (srv *Server) PrintCountQueue() {
	for true {
		time.Sleep(5000 * time.Millisecond)
		fmt.Println(srv.Name, "length channel:", len(srv.Queue))
	}
}

func (srv *Server) CountQueue() int {
	return len(srv.Queue)
}

func (srv *Server) ParseQueue() {
	// Парсинг структур
	for true {
		if len(srv.Queue) > 0 {
			for i := 0; i < srv.ProcSpeed; i++ {
				//<- srv.Queue
				tmp := <-srv.Queue
				fmt.Println(srv.Name, "обработана структура:", tmp, tmp.Price, tmp.Method)
			}
			//fmt.Println(srv.Name, "Цикл обработки прошел.")
		}
		time.Sleep(1000 * time.Millisecond) // Парсинг происходит ProcSpeed структур в секунду
	}
}

func (srv *Server) Init(name string) {
	srv.Queue = make(chan m.Message, 10000)
	srv.ReadConf(name)
}

func (srv *Server) ReadConf(name string) {
	cfg, err := ini.Load("conf_server.ini")
	if err != nil {
		fmt.Printf("Fail to read file: %v", err)
		os.Exit(1)
	}

	//delay, _ := cfg.Section(name).Key("delay").Int()
	srv.ProcSpeed, err = cfg.Section(name).Key("proc_speed").Int()
	if err != nil {
		fmt.Printf("Fail to read file: ", err)
		os.Exit(1)
	}
	srv.Port = cfg.Section(name).Key("port").String()
	srv.Name = cfg.Section(name).Key("name").String()
	srv.server_id, _ = cfg.Section(name).Key("server_id").Int()
}

func (srv *Server) SendMessage(srv_dest Server, n int) error {
	ip := "127.0.0.1" + srv_dest.Port
	conn, err := net.Dial("tcp", ip)

	if err != nil {
		fmt.Println(err)
		return err
	}

	defer conn.Close()

	messages := make([]m.Message, n)
	for i := 0; i < n; i++ {
		tmp := <-srv.Queue
		messages[i] = tmp
	}

	b, err := json.Marshal(messages)
	if err != nil {
		fmt.Println(err)
		return err
	}

	if n, err := conn.Write(b); n == 0 || err != nil {
		fmt.Println(err)
		return err
	}

	name1 := srv.Name
	name2 := srv_dest.Name
	fmt.Println("Отправлено структур: from", name1, "to", name2, ":", n)
	return nil
}

func main() {
	for i := 1; i <= N; i++ {
		srv := new(Server)
		srv.Init("server" + strconv.Itoa(i))
		Servers[i-1] = *srv
	}

	time.Sleep(1000 * time.Millisecond)

	for i := 0; i < N; i++ {
		go Servers[i].Listen()
	}

	for {
	}
}
