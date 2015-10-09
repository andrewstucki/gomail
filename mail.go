package main

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/jordan-wright/email"
	"log"
	"net"
	"strings"
)

type Mail struct {
	Body      string `json:"-"`
	To        string `json:"to"`
	Id        string `json:"id"`
	From      string `json:"-"`
	Subject   string `json:"-"`
	MessageId string `json:"-"`
}

func (m *Mail) constructMessage() ([]byte, error) {
	message := email.NewEmail()
	message.From = m.From
	message.To = []string{m.To}
	message.Subject = m.Subject
	message.HTML = []byte(m.Body)
	return message.Bytes()
}

func (m *Mail) Send() {
	var err error
	var servers = make([]string, 0)

	mailTokens := strings.Split(m.To, "@")
	domain := mailTokens[len(mailTokens)-1]

	err = Database.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("Messages"))
		response := bucket.Get([]byte(m.MessageId))
		if response == nil {
			return fmt.Errorf("No message found with that id")
		}
		m.Body = string(response)
		return nil
	})
	if err != nil {
		EventQueue <- Event{RecipientId: m.Id, MessageId: m.MessageId, Status: "failed"}
		return
	}

	data, err := m.constructMessage()
	if err != nil {
		log.Println(err)
		EventQueue <- Event{RecipientId: m.Id, MessageId: m.MessageId, Status: "failed"}
		// error handling here
		return
	}

	Database.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("MXRecords"))
		response := bucket.Get([]byte(domain))
		if response == nil {
			return nil
		}
		serversString := string(response)
		servers = strings.Split(serversString, ",")
		return nil
	})

	if len(servers) == 0 {
		mxServers, err := net.LookupMX(domain)
		if err != nil {
			// error handler here
			EventQueue <- Event{RecipientId: m.Id, MessageId: m.MessageId, Status: "failed"}
			return
		}
		for _, server := range mxServers {
			servers = append(servers, fmt.Sprintf("%s:25", strings.TrimRight(server.Host, ".")))
		}
		Database.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("MXRecords"))
			bucket.Put([]byte(domain), []byte(strings.Join(servers, ","))) // ignore the error since we don't want to die on a cache fail
			return nil
		})
	}

	for _, server := range servers {
		err = SendMail(
			30,
			server,
			m.From,
			m.To,
			data,
		)
		if err == nil {
			break
		}
	}

	if err == nil || err.Error() == "250 OK" {
		// callback for recipient success
		EventQueue <- Event{RecipientId: m.Id, MessageId: m.MessageId, Status: "success"}
	} else {
		// callback for recipient failure
		EventQueue <- Event{RecipientId: m.Id, MessageId: m.MessageId, Status: "failed"}
		log.Printf("Failed to sent message to %s: %s\n", m.To, err)
	}
}
