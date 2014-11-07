package main

import (
  "fmt"
  "log"
  "net"
  "os"
  "os/signal"
  "syscall"
  "github.com/takama/daemon"
  "time"
  //"net/http"
  //"io/ioutil"
  //"github.com/bitly/go-simplejson"
  "database/sql"
   _ "github.com/go-sql-driver/mysql"
  //"encoding/json"
  //"strconv"
  //"strings"
  //"reflect"
)

const (
  name        = "btc_t"
  description = "BTC AUTO TRADE SERVICE"
  port = ":9979"

  minute_5 = 300
  minute_30 = 1800
)

type K struct {
 o float32
 c float32
 h float32
 l float32
 v float32
 date int
}

// Service has embedded daemon
type Service struct {
  daemon.Daemon
}

// Manage by daemon commands or run the daemon
func (service *Service) Manage() (string, error) {

  usage := "Usage: myservice install | remove | start | stop | status"

  // if received any kind of command, do it
  if len(os.Args) > 1 {
    command := os.Args[1]
    switch command {
    case "install":
      return service.Install()
    case "remove":
      return service.Remove()
    case "start":
      return service.Start()
    case "stop":
      return service.Stop()
    case "status":
      return service.Status()
    default:
      return usage, nil
    }
  }

  // Do something, call your goroutines, etc

  // Set up channel on which to send signal notifications.
  // We must use a buffered channel or risk missing the signal
  // if we're not ready to receive when the signal is sent.
  interrupt := make(chan os.Signal, 1)
  signal.Notify(interrupt, os.Interrupt, os.Kill, syscall.SIGTERM)

  // Set up listener for defined host and port
  listener, err := net.Listen("tcp", port)
  if err != nil {
    return "Possibly was a problem with the port binding", err
  }

  // set up channel on which to send accepted connections
  listen := make(chan net.Conn, 100)
  go acceptConnection(listener, listen)

  t_flag := make(chan bool, 1)

  // loop work cycle with accept connections or interrupt
  // by system signal
  for {
    time.Sleep(5 * time.Second)
    t_flag <- true
    t()
    <-t_flag
  }

  // never happen, but need to complete code
  return usage, nil
}

func t() {
  // db connect 
  db, err := sql.Open("mysql", "btc:btc123@tcp(localhost:3306)/btc?charset=utf8")
  if err != nil {
    log.Println("MYSQL CONNECT ERROR!")
    log.Println(err)
    return
  }
  defer db.Close()

  // get last row
  rows, err := db.Query("SELECT id, cast(date as unsigned), h, l, o, c FROM btc.k_5_minute order by date desc limit 60")
  if err != nil {
    log.Println("GET LAST ROW ERROR!")
    log.Println(err)
    return
  }
  data := make([]K, 60)
  n := 0
  var ma60 float32
  for rows.Next() {
    var id int
    var date int
    var h float32
    var l float32
    var o float32
    var c float32
    var v float32
    err = rows.Scan(&id, &date, &h, &l, &o, &c)
    if err != nil {
      log.Println("QUERY LAST k_5 ERROR!")
      log.Println(err)
      return
    }
    k := K{o: o, c: c, l: l, h:h, date: date, v: v}
    data = append(data, k)
    n += 1
    ma60 += c
  }
  ma60 /= 60.0

  if n < 60 {
    log.Println("WAITING FOR DATA, LESS THAN 60 K!")
    return
  }

  cur_time := int(time.Now().Unix())

  if cur_time - data[0].date > 300 {
    log.Println("WAITING FOR DATA!")
    return
  }

  var h_24 float32
  for _, v := range data[1:24] {
    if h_24 < v.h {
      h_24 = v.h
    }
  }
  var l_12 float32 = 1000000.0
  for _, v := range data[1:12] {
    if v.l < l_12 {
      l_12 = v.l
    }
  }

  if data[0].c < ma60 {
    log.Println("BEAR, WAITING FOR CHANCE!")
  }

  if data[0].c > h_24 {
    log.Println("BUY@" + fmt.Sprintf("%f", data[0].c))
  }

  if data[0].c < l_12 {
    log.Println("SELL@" + fmt.Sprintf("%f", data[0].c))
  }

}

// Accept a client connection and collect it in a channel
func acceptConnection(listener net.Listener, listen chan<- net.Conn) {
  for {
    conn, err := listener.Accept()
    if err != nil {
      continue
    }
    listen <- conn
  }
}

func handleClient(client net.Conn) {
  for {
    buf := make([]byte, 4096)
    numbytes, err := client.Read(buf)
    if numbytes == 0 || err != nil {
      return
    }
    client.Write(buf)
  }
}

func main() {
  srv, err := daemon.New(name, description)
  if err != nil {
    fmt.Println("Error: ", err)
    os.Exit(1)
  }
  service := &Service{srv}
  status, err := service.Manage()
  if err != nil {
    fmt.Println(status, "\nError: ", err)
    os.Exit(1)
  }
  fmt.Println(status)
}
