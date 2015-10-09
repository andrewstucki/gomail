package main

import (
	"crypto/tls"
	"fmt"
	"net/smtp"
	"time"
)

func SendMail(timeout time.Duration, addr string, from string, to string, msg []byte) error {
	response := make(chan error, 1)
	var conn *smtp.Client
	var err error

	go func() {
		conn, err = smtp.Dial(addr)
		if err != nil {
			response <- err
			return
		}
		response <- nil
	}()

	select {
	case res := <-response:
		if res == nil {
			go func() {
				defer conn.Close()
				if err = conn.Hello("localhost"); err != nil {
					response <- err
					return
				}
				if ok, _ := conn.Extension("STARTTLS"); ok {
					config := &tls.Config{ServerName: addr}
					if err = conn.StartTLS(config); err != nil {
						response <- err
						return
					}
				}
				if err = conn.Mail(from); err != nil {
					response <- err
					return
				}
				if err = conn.Rcpt(to); err != nil {
					response <- err
					return
				}
				w, err := conn.Data()
				if err != nil {
					response <- err
					return
				}
				_, err = w.Write(msg)
				if err != nil {
					response <- err
					return
				}
				err = w.Close()
				if err != nil {
					response <- err
					return
				}
				response <- conn.Quit()
			}()
			return <-response
		} else {
			return res
		}
	case <-time.After(time.Second * timeout): //don't do the smtp transaction, abandon the socket, it'll timeout after ~3 mins in syn_sent
		return fmt.Errorf("Sending timeout")
	}
}
