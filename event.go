package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"sync"
	"time"
)

var EventQueue chan Event
var messageCounts map[string]int
var lock = &sync.Mutex{}

type Event struct {
	MessageId   string
	RecipientId string
	Status      string
}

func flushMessageCounts() {
	lock.Lock()
	defer lock.Unlock()
	err := Database.Update(func(tx *bolt.Tx) error {
		var count uint32
		bucket := tx.Bucket([]byte("Messages"))

		for key, value := range messageCounts {
			messageCountKey := []byte(fmt.Sprintf("%s:Count", key))
			response := bucket.Get(messageCountKey)

			if response == nil {
				return fmt.Errorf("Unable to find proper message id")
			}
			reader := bytes.NewBuffer(response)
			binary.Read(reader, binary.LittleEndian, &count)
			count -= uint32(value)

			log.Printf("Message: %s, %d sends to go!\n", key, count)

			if count == 0 {
				// callback for message finished
				log.Printf("Message %s finished sending\n", key)
				delete(messageCounts, key)
			}

			updatedCount := make([]byte, 4)
			binary.LittleEndian.PutUint32(updatedCount, count)

			err := bucket.Put(messageCountKey, updatedCount)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		for key, _ := range messageCounts {
			messageCounts[key] = 0
		}
	}
}

func EventWorker() {
	messageCounts = make(map[string]int)
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case event := <-EventQueue:
				lock.Lock()
				if event.Status == "starting" {
					messageCounts[event.MessageId] = 0
				} else {
					messageCounts[event.MessageId] += 1
				}
				lock.Unlock()
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ticker.C:
				flushMessageCounts()
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}
