// Package main provides the entry point for the dispatcher service.
package main

import (
	"log"

	"github.com/CodeClarityCE/utility-boilerplates"
	amqp "github.com/rabbitmq/amqp091-go"
)

// DispatcherService wraps the ServiceBase with dispatcher-specific functionality
type DispatcherService struct {
	*boilerplates.ServiceBase
	dependencyResolver *DependencyResolver
}

// CreateDispatcherService creates a new DispatcherService
func CreateDispatcherService() (*DispatcherService, error) {
	base, err := boilerplates.CreateServiceBase()
	if err != nil {
		return nil, err
	}

	service := &DispatcherService{
		ServiceBase: base,
	}

	// Initialize dependency resolver
	service.dependencyResolver = CreateDependencyResolver()
	err = service.dependencyResolver.LoadPluginConfigurations(service.DB.Plugins)
	if err != nil {
		log.Printf("Warning: Failed to load plugin configurations: %v", err)
		log.Printf("Continuing without dependency resolution")
	} else {
		log.Printf("Dependency resolver initialized successfully")
	}

	// Setup queue handlers
	service.AddQueue("api_request", true, service.handleAPIRequest)
	service.AddQueue("downloader_dispatcher", true, service.handleDownloaderMessage)
	service.AddQueue("plugins_dispatcher", true, service.handlePluginMessage)

	return service, nil
}

// handleAPIRequest handles messages from API
func (s *DispatcherService) handleAPIRequest(d amqp.Delivery) {
	dispatch("api_request", d, s.dependencyResolver, s.ServiceBase)
}

// handleDownloaderMessage handles messages from downloader
func (s *DispatcherService) handleDownloaderMessage(d amqp.Delivery) {
	dispatch("downloader_dispatcher", d, s.dependencyResolver, s.ServiceBase)
}

// handlePluginMessage handles messages from plugins
func (s *DispatcherService) handlePluginMessage(d amqp.Delivery) {
	dispatch("plugins_dispatcher", d, s.dependencyResolver, s.ServiceBase)
}

func Dispatcher() {
	service, err := CreateDispatcherService()
	if err != nil {
		log.Fatalf("Failed to create dispatcher service: %v", err)
	}
	defer service.Close()

	log.Printf("Starting Dispatcher Service...")
	if err := service.StartListening(); err != nil {
		log.Fatalf("Failed to start listening: %v", err)
	}

	log.Printf("Dispatcher Service started")
	service.WaitForever()
}

func main() {
	Dispatcher()
}
