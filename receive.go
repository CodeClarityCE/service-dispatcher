package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"time"

	types_amqp "github.com/CodeClarityCE/utility-types/amqp"
	codeclarity "github.com/CodeClarityCE/utility-types/codeclarity_db"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
)

// receiveMessage receives messages from a RabbitMQ queue and dispatches them for processing.
// It establishes a connection to RabbitMQ, opens a channel, declares a queue, and consumes messages from the queue.
// Each received message is passed to the dispatch function for further processing.
// The function runs indefinitely until interrupted by a signal.
//
// Parameters:
// - connection: The name of the RabbitMQ queue to consume messages from.
//
// Example usage:
// receiveMessage("my_queue")
func receiveMessage(connection string) {
	// Create connexion
	url := ""
	protocol := os.Getenv("AMQP_PROTOCOL")
	if protocol == "" {
		protocol = "amqp"
	}
	host := os.Getenv("AMQP_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("AMQP_PORT")
	if port == "" {
		port = "5672"
	}
	user := os.Getenv("AMQP_USER")
	if user == "" {
		user = "guest"
	}
	password := os.Getenv("AMQP_PASSWORD")
	if password == "" {
		password = "guest"
	}
	url = protocol + "://" + user + ":" + password + "@" + host + ":" + port + "/"

	conn, err := amqp.Dial(url)
	if err != nil {
		failOnError(err, "Failed to connect to RabbitMQ")
	}
	defer conn.Close()

	// Open channel
	ch, err := conn.Channel()
	if err != nil {
		failOnError(err, "Failed to open a channel")
	}
	defer ch.Close()

	// Declare queue
	q, err := ch.QueueDeclare(
		connection, // name
		true,       // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		failOnError(err, "Failed to declare a queue")
	}

	// Consume messages
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		failOnError(err, "Failed to register a consumer")
	}

	var forever = make(chan struct{})
	go func() {
		for d := range msgs {
			// Start timer
			start := time.Now()

			dispatch(connection, d)

			// Print time elapsed
			t := time.Now()
			elapsed := t.Sub(start)
			log.Println(elapsed)
		}
	}()

	log.Printf(" [*] DISPATCHER Waiting for messages on " + connection + ". To exit press CTRL+C")
	<-forever
}

// dispatch is a function that handles the dispatching of messages based on the connection type.
// It takes a connection string and an amqp.Delivery object as parameters.
// If the connection is "api_request", it reads the message from the API, opens the database,
// retrieves the analysis and analyzer documents, initializes the analysis, and sends a message
// to the downloader_dispatcher to download projects.
// If the connection is "downloader_dispatcher", it reads the message from the API, opens the database,
// retrieves the analysis document, starts each plugin in step 0 by sending a message to the dispatcher_plugin,
// and updates the analysis document accordingly.
// If the connection is "plugins_dispatcher", it reads the message, opens the database,
// retrieves the analysis document, checks if the current stage is completed, and if so,
// goes to the next stage and starts each plugin in the new stage by sending a message to the dispatcher_plugin.
// The function also handles error logging and transaction commits.
func dispatch(connection string, d amqp.Delivery) {
	host := os.Getenv("PG_DB_HOST")
	if host == "" {
		log.Printf("PG_DB_HOST is not set")
		return
	}
	port := os.Getenv("PG_DB_PORT")
	if port == "" {
		log.Printf("PG_DB_PORT is not set")
		return
	}
	user := os.Getenv("PG_DB_USER")
	if user == "" {
		log.Printf("PG_DB_USER is not set")
		return
	}
	password := os.Getenv("PG_DB_PASSWORD")
	if password == "" {
		log.Printf("PG_DB_PASSWORD is not set")
		return
	}
	name := os.Getenv("PG_DB_NAME")
	if name == "" {
		log.Printf("PG_DB_NAME is not set")
		return
	}
	if connection == "api_request" { // If message is from api_request
		// Read message from API - handle both string and UUID formats
		var rawMessage map[string]interface{}
		json.Unmarshal([]byte(d.Body), &rawMessage)
		
		// Debug: print the entire message to see what we're receiving
		log.Printf("Debug: Received message: %+v", rawMessage)
		
		// Parse analysis_id as string first, then convert to UUID
		analysis_id_str, ok := rawMessage["analysis_id"].(string)
		if !ok {
			log.Printf("Error: analysis_id is not a string, got: %T %+v", rawMessage["analysis_id"], rawMessage["analysis_id"])
			return
		}
		analysis_id, err := uuid.Parse(analysis_id_str)
		if err != nil {
			log.Printf("Error parsing analysis_id: %v", err)
			return
		}

		dsn := "postgres://" + user + ":" + password + "@" + host + ":" + port + "/" + name + "?sslmode=disable"
		sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn), pgdriver.WithTimeout(50*time.Second)))
		db := bun.NewDB(sqldb, pgdialect.New())
		defer db.Close()

		analysis_document := &codeclarity.Analysis{
			Id: analysis_id,
		}

		ctx := context.Background()
		err = db.NewSelect().Model(analysis_document).WherePK().Scan(ctx)
		if err != nil {
			panic(err)
		}

		analyzer_document := &codeclarity.Analyzer{
			Id: analysis_document.AnalyzerId,
		}
		err = db.NewSelect().Model(analyzer_document).WherePK().Scan(ctx)
		if err != nil {
			panic(err)
		}

		// Initialize analysis
		analysis_document.Stage = 0
		analysis_document.Steps = analyzer_document.Steps
		analysis_document.Status = codeclarity.STARTED
		_, err = db.NewUpdate().Model(analysis_document).WherePK().Exec(ctx)

		if err != nil {
			panic(err)
		}

		// Parse other required fields from raw message
		project_id_str, ok := rawMessage["project_id"].(string)
		if !ok {
			log.Printf("Error: project_id is not a string, got: %T %+v", rawMessage["project_id"], rawMessage["project_id"])
			return
		}
		project_id, err := uuid.Parse(project_id_str)
		if err != nil {
			log.Printf("Error parsing project_id: %v", err)
			return
		}

		organization_id_str, ok := rawMessage["organization_id"].(string)
		if !ok {
			log.Printf("Error: organization_id is not a string")
			return
		}
		organization_id, err := uuid.Parse(organization_id_str)
		if err != nil {
			log.Printf("Error parsing organization_id: %v", err)
			return
		}

		// Parse integration_id (can be null)
		var integration_id uuid.UUID
		if integration_id_raw := rawMessage["integration_id"]; integration_id_raw != nil {
			if integration_id_str, ok := integration_id_raw.(string); ok && integration_id_str != "" {
				integration_id, err = uuid.Parse(integration_id_str)
				if err != nil {
					log.Printf("Error parsing integration_id: %v", err)
					return
				}
			}
		}

		// If integration is set, send message to downloader_dispatcher
		if integration_id != uuid.Nil {
			dispatcherMessage := types_amqp.DispatcherDownloaderMessage{
				AnalysisId:     analysis_id,
				ProjectId:      project_id,
				IntegrationId:  integration_id,
				OrganizationId: organization_id,
			}

			data, _ := json.Marshal(dispatcherMessage)

			// Send message to downloader_dispatcher to download projects
			send("dispatcher_downloader", data)
		} else {
			// Parse config from raw message
			var config map[string]interface{}
			if configRaw := rawMessage["config"]; configRaw != nil {
				config, _ = configRaw.(map[string]interface{})
			}

			// For each plugin in step 0
			for step_id, step := range analysis_document.Steps[0] {
				// Start plugin by sending message to dispatcher_plugin
				dispatcherMessage := types_amqp.DispatcherPluginMessage{
					AnalysisId:     analysis_document.Id,
					OrganizationId: organization_id,
					Data:           config,
				}
				data, _ := json.Marshal(dispatcherMessage)
				analysis_document.Steps[0][step_id].Status = codeclarity.STARTED

				err := db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err = tx.NewUpdate().Model(analysis_document).WherePK().Exec(ctx)
					return err
				})

				if err != nil {
					panic(err)
				}
				send("dispatcher_"+step.Name, data)
			}
		}

	} else if connection == "downloader_dispatcher" { // If message is from api_request
		// Read message from API
		var apiMessage types_amqp.DownloaderDispatcherMessage
		json.Unmarshal([]byte(d.Body), &apiMessage)
		analysis_id := apiMessage.AnalysisId

		dsn := "postgres://" + user + ":" + password + "@" + host + ":" + port + "/" + name + "?sslmode=disable"
		sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn), pgdriver.WithTimeout(50*time.Second)))
		db := bun.NewDB(sqldb, pgdialect.New())
		defer db.Close()
		// Get analysis
		analysis_document := &codeclarity.Analysis{
			Id: analysis_id,
		}
		ctx := context.Background()
		err := db.NewSelect().Model(analysis_document).WherePK().Scan(ctx)
		if err != nil {
			panic(err)
		}

		// For each plugin in step 0
		for step_id, step := range analysis_document.Steps[0] {
			// Start plugin by sending message to dispatcher_plugin
			dispatcherMessage := types_amqp.DispatcherPluginMessage{
				AnalysisId:     analysis_document.Id,
				OrganizationId: apiMessage.OrganizationId,
			}
			data, _ := json.Marshal(dispatcherMessage)
			analysis_document.Steps[0][step_id].Status = codeclarity.STARTED

			err := db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
				_, err = tx.NewUpdate().Model(analysis_document).WherePK().Exec(ctx)
				return err
			})

			if err != nil {
				panic(err)
			}
			send("dispatcher_"+step.Name, data)
		}

		// Commit transaction
		// err = db.CommitTransaction(tctx, trxid, nil)
		// if err != nil {
		// 	log.Printf("Failed to commit transaction: %t", err)
		// 	return
		// }

	} else if connection == "plugins_dispatcher" { // If message is from sbom_dispatcher
		// Read message
		var pluginMessage types_amqp.PluginDispatcherMessage
		json.Unmarshal([]byte(d.Body), &pluginMessage)

		// Open DB
		dsn := "postgres://" + user + ":" + password + "@" + host + ":" + port + "/" + name + "?sslmode=disable"
		sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn), pgdriver.WithTimeout(50*time.Second)))
		db := bun.NewDB(sqldb, pgdialect.New())
		defer db.Close()

		// Get analysis
		analysis_document := &codeclarity.Analysis{
			Id: pluginMessage.AnalysisId,
		}
		ctx := context.Background()
		err := db.NewSelect().Model(analysis_document).WherePK().Scan(ctx)
		if err != nil {
			panic(err)
		}

		// SBOM can trigger an update of the DB
		// Wait for analysis to finish updating db
		for analysis_document.Status == codeclarity.UPDATING_DB {
			time.Sleep(10 * time.Second)
			log.Printf("Waiting for analysis to finish updating db")
			err = db.NewSelect().Model(analysis_document).WherePK().Scan(ctx)
			if err != nil {
				panic(err)
			}
		}

		// Check if stage completed
		stage_completed := true
		if analysis_document.Stage > len(analysis_document.Steps)-1 {
			return
		}
		for step_id := range analysis_document.Steps[analysis_document.Stage] {
			// Check if all steps are completed
			if analysis_document.Steps[analysis_document.Stage][step_id].Status != codeclarity.SUCCESS {
				stage_completed = false
			}
			if analysis_document.Steps[analysis_document.Stage][step_id].Status == codeclarity.FAILURE {
				analysis_document.Status = codeclarity.FAILURE
				_, err = db.NewUpdate().Model(analysis_document).WherePK().Exec(ctx)
				if err != nil {
					panic(err)
				}
				// // Commit transaction
				// err = db.CommitTransaction(tctx, trxid, nil)
				// if err != nil {
				// 	log.Printf("Failed to commit transaction: %t", err)
				// 	return
				// }
				return
			}
		}
		// If stage completed, go to next stage
		if stage_completed {
			// Increment stage
			analysis_document.Stage++
			if analysis_document.Stage == len(analysis_document.Steps) {
				// Analysis completed
				analysis_document.Status = codeclarity.COMPLETED
				_, err = db.NewUpdate().Model(analysis_document).WherePK().Exec(ctx)
				if err != nil {
					panic(err)
				}
			} else {
				// For each plugin in step 0
				for step_id, step := range analysis_document.Steps[analysis_document.Stage] {
					// Start plugin by sending message to dispatcher_plugin
					dispatcherMessage := types_amqp.DispatcherPluginMessage{
						AnalysisId:     pluginMessage.AnalysisId,
						OrganizationId: analysis_document.OrganizationId,
					}
					data, _ := json.Marshal(dispatcherMessage)
					send("dispatcher_"+step.Name, data)
					analysis_document.Steps[analysis_document.Stage][step_id].Status = codeclarity.STARTED
					// TODO create transaction or check if this is atomic
					_, err = db.NewUpdate().Model(analysis_document).WherePK().Exec(ctx)
					if err != nil {
						panic(err)
					}
				}
			}
		}

	}
}
