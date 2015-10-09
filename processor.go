package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"os"
)

type ProcessorPool struct {
	Pool       chan chan string
	maxWorkers int
}

var ProcessQueue chan string

type Processor struct {
	Pool           chan chan string
	ProcessChannel chan string
	quit           chan bool
}

func handleRecipient(message *Message, recipientText string) {
	if recipientText != "" {
		mail := Mail{MessageId: message.Id, From: message.From, Subject: message.Subject}
		err := json.Unmarshal([]byte(recipientText), &mail)
		if err != nil {
			log.Println(err)
		} else {
			JobQueue <- Job{Message: mail}
		}
	}
}

func process(fileName string) {
	var message *Message
	var err error

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if message == nil {
			message = &Message{}

			err = json.Unmarshal([]byte(scanner.Text()), message)
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("Sending message %s to %d recipients!\n", message.Id, message.RecipientCount)
			EventQueue <- Event{RecipientId: "", MessageId: message.Id, Status: "starting"}

			err = Database.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket([]byte("Messages"))
				count := make([]byte, 4)
				binary.LittleEndian.PutUint32(count, message.RecipientCount)
				err := bucket.Put([]byte(fmt.Sprintf("%s:Count", message.Id)), count)
				if err != nil {
					return err
				}
				return bucket.Put([]byte(message.Id), []byte(message.Body))
			})

			if err != nil {
				log.Fatal(err)
			}

		} else {
			handleRecipient(message, scanner.Text())
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func NewProcessor(pool chan chan string) Processor {
	return Processor{
		Pool:           pool,
		ProcessChannel: make(chan string),
		quit:           make(chan bool),
	}
}

func (p Processor) Start() {
	go func() {
		for {
			p.Pool <- p.ProcessChannel

			select {
			case fileName := <-p.ProcessChannel:
				process(fileName)
			case <-p.quit:
				return
			}
		}
	}()
}

func (p Processor) Stop() {
	go func() {
		p.quit <- true
	}()
}

func NewProcessorPool(maxWorkers int) *ProcessorPool {
	pool := make(chan chan string, maxWorkers)
	return &ProcessorPool{Pool: pool, maxWorkers: maxWorkers}
}

func (p *ProcessorPool) Run() {
	for i := 0; i < p.maxWorkers; i++ {
		processor := NewProcessor(p.Pool)
		processor.Start()
	}
	go p.dispatch()
}

func (p *ProcessorPool) dispatch() {
	for {
		select {
		case request := <-ProcessQueue:
			go func(fileName string) {
				processorChannel := <-p.Pool
				processorChannel <- fileName
			}(request)
		}
	}
}
