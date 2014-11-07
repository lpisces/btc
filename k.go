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
  "strconv"
  "strings"
  //"reflect"
)

const (
  name        = "btc_k"
  description = "BTC AUTO TRADE SERVICE"
  port = ":9978"

  minute_5 = 300
  minute_30 = 1800
)

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

  k_flag := make(chan bool, 1)

  // loop work cycle with accept connections or interrupt
  // by system signal
  for {
    time.Sleep(5 * time.Second)
    k_flag <- true
    k()
    <-k_flag
  }

  // never happen, but need to complete code
  return usage, nil
}

func k() {
  // db connect 
  db, err := sql.Open("mysql", "btc:btc123@tcp(localhost:3306)/btc?charset=utf8")
  if err != nil {
    log.Println("MYSQL CONNECT ERROR!")
    log.Println(err)
    return
  }
  defer db.Close()


  // get last row
  rows, err := db.Query("SELECT id, cast(date as unsigned), h, l, o, c FROM btc.k_5_minute order by date desc limit 1")
  if err != nil {
    log.Println("GET LAST ROW ERROR!")
    log.Println(err)
    return
  }
  last_date := 0
  for rows.Next() {
    var id int
    var date int
    var h string
    var l string
    var o string
    var c string
    err = rows.Scan(&id, &date, &h, &l, &o, &c)
    if err != nil {
      log.Println("QUERY LAST k_5 ERROR!")
      log.Println(err)
      return
    }
    last_date = date
  }

  cur_time := int(time.Now().Unix())
  if last_date == 0 {
    last_date = cur_time - cur_time%(5*60) 
  } else {
    last_date += 300
    if last_date > cur_time {
      log.Println("WAITING FOR DATA!")
      return
    }
  }

  // get last row
  last_row, err := db.Query("SELECT id, date, price, amount, cast(tid as unsigned), ttype FROM btc.history order by tid desc limit 1")
  if err != nil {
    log.Println("GET LAST ROW ERROR!")
    log.Println(err)
    return
  }
  l_date := 0
  for last_row.Next() {
    var id int
    var tid int
    var date int
    var price string
    var amount string
    var ttype string
    err = last_row.Scan(&id, &date, &price, &amount, &tid, &ttype)
    if err != nil {
      log.Println("QUERY LAST ROW ERROR!")
      log.Println(err)
      return
    }
    l_date = date
  }
  if l_date < last_date {
    log.Println("HISTORY DATA MISSING!")
    return
  }

  // get last rows
  rows2, err := db.Query(fmt.Sprintf("SELECT id, date, price, amount, cast(tid as unsigned), ttype FROM btc.history where date > %d and date < %d order by tid desc", last_date-300, last_date))
  if err != nil {
    log.Println("GET LAST HISTORY ERROR!")
    log.Println(err)
    return
  }
  log.Println(fmt.Sprintf("SELECT id, date, price, amount, cast(tid as unsigned), ttype FROM btc.history where date > %d and date < %d order by tid asc", last_date-300, last_date))
  o, c, h, l, v:= 0.0, 0.0, 0.0, 10000000.0, 0.0
  tmp := 0.0
  for rows2.Next() {
    var id int
    var tid int
    var date int
    var price string
    var amount string
    var ttype string
    err = rows2.Scan(&id, &date, &price, &amount, &tid, &ttype)
    if err != nil {
      log.Println("QUERY LAST ROW ERROR!")
      log.Println(err)
      return
    }
    p, _:= strconv.ParseFloat(strings.Trim(price, " "), 64)
    if o == 0.0 {
      o = p
    }
    if p > h {
      h = p
    }
    if p < l {
      l = p
    }
    a, _:= strconv.ParseFloat(strings.Trim(amount, " "), 64)
    v += a
    tmp = p
  }
  c = tmp
  log.Println(fmt.Sprintf("%f, %f, %f, %f, %f", o, c, h, l, v))

  stmt, err := db.Prepare("INSERT into btc.k_5_minute set date=?,o=?,c=?,h=?,l=?,v=?")
  if err != nil {
    log.Println("MYSQL FORMAT INSERT ERROR!")
    log.Println(err)
    return
  }

  res, err := stmt.Exec(last_date, o, c, h, l, v)
  if err != nil {
    log.Println("MYSQL INSERT ERROR!")
    log.Println(err)
    return
  }
  if r, _ := res.RowsAffected(); r> 0{
    log.Println("OPERATION SUCCEED!")
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
