package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/CodeClarityCE/utility-boilerplates"
	types_amqp "github.com/CodeClarityCE/utility-types/amqp"
	codeclarity "github.com/CodeClarityCE/utility-types/codeclarity_db"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/uptrace/bun"
)

// startPluginsWithDependencyResolution starts plugins in the given stage using dependency resolution
func startPluginsWithDependencyResolution(analysis *codeclarity.Analysis, stageIndex int, organizationId uuid.UUID, config map[string]interface{}, db *bun.DB, dependencyResolver *DependencyResolver, service *boilerplates.ServiceBase) error {
	ctx := context.Background()

	if dependencyResolver == nil {
		log.Printf("Warning: Dependency resolver not initialized, falling back to parallel execution")
		return startAllPluginsInStage(analysis, stageIndex, organizationId, config, db, service)
	}

	log.Printf("Starting stage %d with dependency resolution", stageIndex)

	// Get plugins that are ready to run (dependencies satisfied)
	readyPlugins, err := dependencyResolver.GetReadyPlugins(analysis, stageIndex)
	if err != nil {
		log.Printf("Error getting ready plugins: %v", err)
		return startAllPluginsInStage(analysis, stageIndex, organizationId, config, db, service)
	}

	if len(readyPlugins) == 0 {
		log.Printf("No plugins ready to run in stage %d", stageIndex)
		return nil
	}

	// Sort plugins topologically within the ready set
	sortedPlugins := dependencyResolver.TopologicalSort(readyPlugins)

	log.Printf("Starting %d plugins in dependency order: %v", len(sortedPlugins), getPluginNames(sortedPlugins))

	// Start plugins in dependency order
	for stepId, step := range analysis.Steps[stageIndex] {
		// Check if this plugin is in the ready list
		for _, readyPlugin := range sortedPlugins {
			if step.Name == readyPlugin.Name {
				log.Printf("Starting plugin %s (dependency-resolved)", step.Name)

				dispatcherMessage := types_amqp.DispatcherPluginMessage{
					AnalysisId:     analysis.Id,
					OrganizationId: organizationId,
					Data:           config,
				}
				data, _ := json.Marshal(dispatcherMessage)
				analysis.Steps[stageIndex][stepId].Status = codeclarity.STARTED

				err := db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, updateErr := tx.NewUpdate().Model(analysis).WherePK().Exec(ctx)
					return updateErr
				})

				if err != nil {
					log.Printf("Error updating analysis for plugin %s: %v", step.Name, err)
					return err
				}

				err = service.SendMessage("dispatcher_"+step.Name, data)
				if err != nil {
					log.Printf("Failed to send message to dispatcher_%s: %v", step.Name, err)
				}
				break
			}
		}
	}

	return nil
}

// startAllPluginsInStage starts all plugins in a stage without dependency resolution (fallback)
func startAllPluginsInStage(analysis *codeclarity.Analysis, stageIndex int, organizationId uuid.UUID, config map[string]interface{}, db *bun.DB, service *boilerplates.ServiceBase) error {
	ctx := context.Background()

	log.Printf("Starting all plugins in stage %d (no dependency resolution)", stageIndex)

	for stepId, step := range analysis.Steps[stageIndex] {
		log.Printf("Starting plugin %s (parallel mode)", step.Name)

		dispatcherMessage := types_amqp.DispatcherPluginMessage{
			AnalysisId:     analysis.Id,
			OrganizationId: organizationId,
			Data:           config,
		}
		data, _ := json.Marshal(dispatcherMessage)
		analysis.Steps[stageIndex][stepId].Status = codeclarity.STARTED

		err := db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
			_, updateErr := tx.NewUpdate().Model(analysis).WherePK().Exec(ctx)
			return updateErr
		})

		if err != nil {
			log.Printf("Error updating analysis for plugin %s: %v", step.Name, err)
			return err
		}

		err = service.SendMessage("dispatcher_"+step.Name, data)
		if err != nil {
			log.Printf("Failed to send message to dispatcher_%s: %v", step.Name, err)
		}
	}

	return nil
}

// getPluginNames extracts plugin names from steps for logging
func getPluginNames(steps []codeclarity.Step) []string {
	names := make([]string, len(steps))
	for i, step := range steps {
		names[i] = step.Name
	}
	return names
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
func dispatch(connection string, d amqp.Delivery, dependencyResolver *DependencyResolver, service *boilerplates.ServiceBase) {
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

		db := service.DB.CodeClarity

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

		// Query project to check its type
		project_document := &codeclarity.Project{
			Id: project_id,
		}
		err = db.NewSelect().Model(project_document).WherePK().Scan(ctx)
		if err != nil {
			log.Printf("Error fetching project: %v", err)
			panic(err)
		}

		// Send to downloader for VCS projects (integration_id set) OR FILE projects
		if integration_id != uuid.Nil || project_document.Type == "FILE" {
			dispatcherMessage := types_amqp.DispatcherDownloaderMessage{
				AnalysisId:     analysis_id,
				ProjectId:      project_id,
				IntegrationId:  integration_id,
				OrganizationId: organization_id,
			}

			data, _ := json.Marshal(dispatcherMessage)

			// Send message to downloader_dispatcher to download/decompress projects
			err = service.SendMessage("dispatcher_downloader", data)
			if err != nil {
				log.Printf("Failed to send message to dispatcher_downloader: %v", err)
			}
		} else {
			// Parse config from raw message
			var config map[string]interface{}
			if configRaw := rawMessage["config"]; configRaw != nil {
				config, _ = configRaw.(map[string]interface{})
			}

			// Start plugins with dependency resolution
			err = startPluginsWithDependencyResolution(analysis_document, 0, organization_id, config, db, dependencyResolver, service)
			if err != nil {
				log.Printf("Error starting plugins in stage 0: %v", err)
				panic(err)
			}
		}

	} else if connection == "downloader_dispatcher" { // If message is from api_request
		// Read message from API
		var apiMessage types_amqp.DownloaderDispatcherMessage
		json.Unmarshal([]byte(d.Body), &apiMessage)
		analysis_id := apiMessage.AnalysisId

		db := service.DB.CodeClarity
		// Get analysis
		analysis_document := &codeclarity.Analysis{
			Id: analysis_id,
		}
		ctx := context.Background()
		err := db.NewSelect().Model(analysis_document).WherePK().Scan(ctx)
		if err != nil {
			panic(err)
		}

		// Start plugins with dependency resolution
		err = startPluginsWithDependencyResolution(analysis_document, 0, apiMessage.OrganizationId, nil, db, dependencyResolver, service)
		if err != nil {
			log.Printf("Error starting plugins in stage 0: %v", err)
			panic(err)
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
		db := service.DB.CodeClarity

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
				// Start plugins with dependency resolution for the next stage
				err = startPluginsWithDependencyResolution(analysis_document, analysis_document.Stage, analysis_document.OrganizationId, nil, db, dependencyResolver, service)
				if err != nil {
					log.Printf("Error starting plugins in stage %d: %v", analysis_document.Stage, err)
					panic(err)
				}
			}
		}

		// Check if there are any plugins in the current or previous stages that might now be ready to run
		if dependencyResolver != nil && dependencyResolver.HasPendingDependentPlugins(analysis_document) {
			log.Printf("Found plugins with satisfied dependencies, checking all stages for ready plugins")

			// Check all stages for plugins that might now be ready
			for stageIndex := 0; stageIndex <= analysis_document.Stage && stageIndex < len(analysis_document.Steps); stageIndex++ {
				readyPlugins, err := dependencyResolver.GetReadyPlugins(analysis_document, stageIndex)
				if err != nil {
					log.Printf("Error checking ready plugins in stage %d: %v", stageIndex, err)
					continue
				}

				if len(readyPlugins) > 0 {
					log.Printf("Starting %d newly ready plugins in stage %d", len(readyPlugins), stageIndex)
					err = startPluginsWithDependencyResolution(analysis_document, stageIndex, analysis_document.OrganizationId, nil, db, dependencyResolver, service)
					if err != nil {
						log.Printf("Error starting ready plugins in stage %d: %v", stageIndex, err)
					}
				}
			}
		}

	}
}
