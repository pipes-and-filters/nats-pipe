package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"runtime"

	nats "github.com/nats-io/go-nats"
	"github.com/pipes-and-filters/filters"

	"log"
)

var (
	chainFile string
	subscribe string
	chain     filters.Chain
	sub       *nats.Subscription
)

func init() {
	flag.StringVar(&chainFile, "chain-file", os.Getenv("NATS_PIPE_CHAIN"), "Chain file for nats pipe")
	flag.StringVar(&subscribe, "subscribe", os.Getenv("SUBSCRIBE"), "Nats stream to subscribe to")
}

func main() {
	flag.Parse()
	if chainFile == "" {
		log.Fatal("No chain file designated")
	}
	if subscribe == "" {
		log.Fatal("No subscription designated")
	}
	var err error
	chain, err = filters.ChainFile("chain.yml")
	if err != nil {
		log.Fatal(err)
	}
	_, err = chain.Exec()
	if err != nil {
		log.Fatal(err)
	}
	var nc *nats.Conn
	nc, err = nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	sub, err = nc.Subscribe(subscribe, FilterHandler)
	if err != nil {
		log.Fatal(err)
	}
	runtime.Goexit()
}

func FilterHandler(m *nats.Msg) {
	fmt.Printf("Received a message: %s\n", string(m.Data))
	e, err := chain.Exec()
	if err != nil {
		log.Print(err)
	}
	e.SetInput(bytes.NewReader(m.Data))
	buf := new(bytes.Buffer)
	e.SetOutput(buf)
	err = e.Run()
	if err != nil {
		logErrors(e.Errors())
		log.Print(err)
	}
	log.Print(buf.String())
}

func logErrors(es []error) {
	for _, e := range es {
		fmt.Println(e)
	}
}
