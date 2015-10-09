package main

import (
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// TODO:
// - DNS expiration go routine
// - Callback handling
// - DKIM
// - DRY up WorkerPool/Queue implementation (See processor.go and worker.go)

var Database *bolt.DB
var dispatcher *Dispatcher
var processorPool *ProcessorPool

type Message struct {
	Body           string `json:"body"`
	Id             string `json:"id"`
	From           string `json:"from"`
	Subject        string `json:"subject"`
	RecipientCount uint32 `json:"recipientCount"`
}

var quit chan os.Signal

func setup() {
	var err error

	quit = make(chan os.Signal, 1)
	JobQueue = make(chan Job, 10000) // JobQueue size + # of workers + # of processors = maximum messages enqueued/delivering
	ProcessQueue = make(chan string)
	EventQueue = make(chan Event)

	signal.Notify(quit, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	dispatcher = NewDispatcher(100) // # of workers * (tcp timeout/smtp timeout [approx 5]) = maximum open sockets
	dispatcher.Run()

	processorPool = NewProcessorPool(10)
	processorPool.Run()

	EventWorker()
	// DNSExpirationWorker()

	Database, err = bolt.Open("cache.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	err = Database.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("MXRecords"))
		if err != nil {
			if err.Error() != "bucket already exists" {
				return fmt.Errorf("create bucket: %s", err)
			}
		}

		bucket := tx.Bucket([]byte("MXRecords"))
		bucket.Put([]byte("sink.localhost"), []byte("127.0.0.1:1025")) // ignore the error since we don't want to die on a cache fail

		_, err = tx.CreateBucket([]byte("Messages"))
		if err != nil {
			if err.Error() != "bucket already exists" {
				return fmt.Errorf("create bucket: %s", err)
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	setup()
	defer Database.Close()

	ProcessQueue <- "messages.txt"
	ProcessQueue <- "messages2.txt"

	<-quit
}
