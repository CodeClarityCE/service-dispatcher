// Package main provides the entry point for the dispatcher service.
package main

import "log"

// Global dependency resolver instance
var dependencyResolver *DependencyResolver

func Dispatcher() {
	// Initialize dependency resolver
	dependencyResolver = NewDependencyResolver()
	err := dependencyResolver.LoadPluginConfigurations()
	if err != nil {
		log.Printf("Warning: Failed to load plugin configurations: %v", err)
		log.Printf("Continuing without dependency resolution")
	} else {
		log.Printf("Dependency resolver initialized successfully")
	}

	forever := make(chan bool)
	go receiveMessage("api_request")
	go receiveMessage("downloader_dispatcher")
	go receiveMessage("plugins_dispatcher")
	<-forever
}

func main() {
	Dispatcher()
}
