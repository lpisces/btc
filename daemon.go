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
  "net/http"
  "io/ioutil"
  "github.com/bitly/go-simplejson"
  "database/sql"
   _ "github.com/go-sql-driver/mysql"
  "encoding/json"
  "strconv"
  //"strings"
  //"reflect"
)

const (
  name        = "btc"
  description = "BTC AUTO TRADE SERVICE"
  port = ":9977"
  api_url = "https://data.btcchina.com/data/historydata"
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

  fetch_flag := make(chan bool, 1)

  // loop work cycle with accept connections or interrupt
  // by system signal
  for {
    time.Sleep(60 * time.Second)
    fetch_flag <- true
    fetch()
    <-fetch_flag
  }

  // never happen, but need to complete code
  return usage, nil
}

func fetch() {
  // db connect 
  db, err := sql.Open("mysql", "btc:btc123@tcp(localhost:3306)/btc?charset=utf8")
  if err != nil {
    log.Println("MYSQL CONNECT ERROR!")
    log.Println(err)
    return
  }
  defer db.Close()

  // get last row
  rows, err := db.Query("SELECT id, date, price, amount, cast(tid as unsigned), ttype FROM btc.history order by tid desc limit 1")
  if err != nil {
    log.Println("GET LAST ROW ERROR!")
    log.Println(err)
    return
  }
  last_tid := 0
  for rows.Next() {
    var id int
    var tid int
    var date int
    var price string
    var amount string
    var ttype string
    err = rows.Scan(&id, &date, &price, &amount, &tid, &ttype)
    if err != nil {
      log.Println("QUERY LAST ROW ERROR!")
      log.Println(err)
      return
    }
    last_tid = tid
  }

  url := api_url
  if (last_tid == 0) {
    url = api_url
  } else {
    tid := strconv.Itoa(last_tid)
    url = api_url + "?since=" + tid + "&limit=500&sincetype=id"
  }
  resp, err := http.Get(url)
  if err != nil {
    log.Println("API ACCESS ERROR!")
    return
  }
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)

  js, err := simplejson.NewJson(body)
  if err != nil {
    log.Println("JSON FORMAT ERROR!")
    return
  }
  arr, err := js.Array()

  for _, v := range arr {
    date :=v.(map[string]interface{})["date"]
    price :=v.(map[string]interface{})["price"]
    amount :=v.(map[string]interface{})["amount"]
    tid :=v.(map[string]interface{})["tid"]
    ttype :=v.(map[string]interface{})["type"]

    //log.Println(date.(string))
    //log.Println(price.(json.Number))
    //log.Println(amount.(json.Number))
    //log.Println(tid.(string))
    //log.Println(ttype.(string))

    stmt, err := db.Prepare("INSERT into btc.history set date=?,price=?,amount=?,tid=?,ttype=?")
    if err != nil {
      log.Println("MYSQL FORMAT INSERT ERROR!")
      log.Println(err)
      return
    }
    res, err := stmt.Exec(date.(string), string(price.(json.Number)), string(amount.(json.Number)), tid.(string), ttype.(string))
    if err != nil {
      log.Println("MYSQL INSERT ERROR!")
      log.Println(err)
      return
    }
    id, err := res.LastInsertId()
    if err != nil {
      log.Println("GET LAST INSERT ID ERROR!")
      log.Println(err)
      return
    }
    log.Printf("LAST HISTORY ID IS %d", id)
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
