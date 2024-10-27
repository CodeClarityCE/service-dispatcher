// Package main provides the entry point for the dispatcher service.
package main

func Dispatcher() {
	forever := make(chan bool)
	go receiveMessage("api_request")
	go receiveMessage("downloader_dispatcher")
	go receiveMessage("plugins_dispatcher")
	<-forever
}

func main() {
	Dispatcher()
}
