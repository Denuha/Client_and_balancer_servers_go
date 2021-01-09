package main

import (
	m "./mess"
	"encoding/json"
	"fmt"
	"gopkg.in/ini.v1"
	"math/rand"
	"net"
	"os"
	"time"
)

var time_sleep = 0
var count_structs = 0

func SendMessages(conn net.Conn) {
	for {
		// Частота отправки - время задержки между запросами
		time.Sleep(time.Duration(time_sleep) * time.Millisecond)

		n_cs := rand.Intn(count_structs) + 1 // Количетсво отправляемых структур
		b := GenerateMessage(n_cs)

		if n, err := conn.Write(b); n == 0 || err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("Отправлено структур:", n_cs)
	}
}

func GenerateMessage(count_structs int) []byte {
	var messages []m.Message
	messages = make([]m.Message, count_structs)

	for i := 0; i < count_structs; i++ {
		m := m.Message{
			Price:    rand.Intn(1000),
			Quantity: rand.Intn(10),
			Amount:   rand.Intn(10),
			Object:   rand.Intn(5),
			Method:   rand.Intn(5),
		}
		messages[i] = m
	}

	b, err := json.Marshal(messages)
	if err != nil {
		fmt.Println(err)
		return make([]byte, 0, 0)
	}
	return b
}

func ReadConf() error {
	cfg, err := ini.Load("conf_client.ini")
	if err != nil {
		fmt.Printf("Fail to read file: %v", err)
		os.Exit(1)
	}

	delay, err := cfg.Section("settings").Key("delay").Int()
	if err != nil {
		fmt.Println(err)
		return err
	}

	cs, _ := cfg.Section("settings").Key("count_structs").Int()
	if err != nil {
		fmt.Println(err)
		return err
	}

	time_sleep = delay
	count_structs = cs
	return nil
}

func main() {
	rand.Seed(time.Now().UnixNano())

	fmt.Println("Чтение конфига...")
	errc := ReadConf()

	if errc != nil {
		fmt.Println(errc)
		return
	}

	fmt.Println("Delay =", time_sleep)
	fmt.Println("count_structs =", count_structs)

	ip := "127.0.0.1:4545"
	conn, err := net.Dial("tcp", ip)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer conn.Close()

	go SendMessages(conn)

	fmt.Println("Connected to", ip)
	for {

	}
}
