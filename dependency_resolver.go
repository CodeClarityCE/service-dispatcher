package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	codeclarity "github.com/CodeClarityCE/utility-types/codeclarity_db"
	plugin_db "github.com/CodeClarityCE/utility-types/plugin_db"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
)

// DependencyResolver handles plugin dependency resolution and scheduling
type DependencyResolver struct {
	pluginConfigs map[string]plugin_db.Plugin
}

// NewDependencyResolver creates a new dependency resolver
func NewDependencyResolver() *DependencyResolver {
	return &DependencyResolver{
		pluginConfigs: make(map[string]plugin_db.Plugin),
	}
}

// LoadPluginConfigurations loads all plugin configurations from the database
func (dr *DependencyResolver) LoadPluginConfigurations() error {
	host := os.Getenv("PG_DB_HOST")
	if host == "" {
		return fmt.Errorf("PG_DB_HOST is not set")
	}
	port := os.Getenv("PG_DB_PORT")
	if port == "" {
		return fmt.Errorf("PG_DB_PORT is not set")
	}
	user := os.Getenv("PG_DB_USER")
	if user == "" {
		return fmt.Errorf("PG_DB_USER is not set")
	}
	password := os.Getenv("PG_DB_PASSWORD")
	if password == "" {
		return fmt.Errorf("PG_DB_PASSWORD is not set")
	}

	// Connect to plugin database
	dsn := "postgres://" + user + ":" + password + "@" + host + ":" + port + "/plugins?sslmode=disable"
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn), pgdriver.WithTimeout(30*time.Second)))
	db := bun.NewDB(sqldb, pgdialect.New())
	defer db.Close()

	// Load all plugin configurations
	var plugins []plugin_db.Plugin
	err := db.NewSelect().Model(&plugins).Scan(context.Background())
	if err != nil {
		return err
	}

	// Index plugins by name
	for _, plugin := range plugins {
		dr.pluginConfigs[plugin.Name] = plugin
		log.Printf("Loaded plugin config: %s (depends on: %v)", plugin.Name, plugin.DependsOn)
	}

	log.Printf("Loaded %d plugin configurations", len(dr.pluginConfigs))
	return nil
}

// GetReadyPlugins returns plugins from the given stage whose dependencies are satisfied
func (dr *DependencyResolver) GetReadyPlugins(analysis *codeclarity.Analysis, stageIndex int) ([]codeclarity.Step, error) {
	if stageIndex >= len(analysis.Steps) {
		return nil, fmt.Errorf("stage %d does not exist (total stages: %d)", stageIndex, len(analysis.Steps))
	}

	var readyPlugins []codeclarity.Step
	stagePlugins := analysis.Steps[stageIndex]

	for _, step := range stagePlugins {
		// Skip if plugin is already started, completed, or failed
		// Only process plugins that haven't been started yet (empty status)
		if step.Status != "" {
			log.Printf("Plugin %s already has status %s, skipping", step.Name, step.Status)
			continue
		}

		// Check if dependencies are satisfied
		if dr.areDependenciesSatisfied(step.Name, analysis) {
			log.Printf("Plugin %s is ready to run (dependencies satisfied)", step.Name)
			readyPlugins = append(readyPlugins, step)
		} else {
			log.Printf("Plugin %s is waiting for dependencies", step.Name)
		}
	}

	return readyPlugins, nil
}

// areDependenciesSatisfied checks if all dependencies for a plugin are completed
// Only checks for dependencies that are actually present in the current analysis
func (dr *DependencyResolver) areDependenciesSatisfied(pluginName string, analysis *codeclarity.Analysis) bool {
	pluginConfig, exists := dr.pluginConfigs[pluginName]
	if !exists {
		log.Printf("Warning: Plugin %s not found in configuration, assuming no dependencies", pluginName)
		return true
	}

	// If no dependencies, plugin is ready
	if len(pluginConfig.DependsOn) == 0 {
		return true
	}

	// Filter dependencies to only include plugins that are actually in this analysis
	actualDependencies := dr.filterDependenciesByAnalysis(pluginConfig.DependsOn, analysis)
	
	if len(actualDependencies) == 0 {
		log.Printf("Plugin %s has no dependencies in current analysis, ready to run", pluginName)
		return true
	}

	log.Printf("Checking dependencies for %s: %v (filtered from %v)", pluginName, actualDependencies, pluginConfig.DependsOn)

	// Check all previous stages for completed dependencies
	dependencyCount := make(map[string]bool)
	for stageIndex := 0; stageIndex < len(analysis.Steps); stageIndex++ {
		for _, step := range analysis.Steps[stageIndex] {
			// Check if this step is a required dependency
			for _, dependency := range actualDependencies {
				if step.Name == dependency {
					if step.Status != codeclarity.SUCCESS {
						log.Printf("Dependency %s is not completed (status: %s) for plugin %s", 
							dependency, step.Status, pluginName)
						return false
					}
					log.Printf("Dependency %s is satisfied for plugin %s", dependency, pluginName)
					dependencyCount[dependency] = true
				}
			}
		}
	}

	// Check if all actual dependencies are satisfied
	for _, dependency := range actualDependencies {
		if !dependencyCount[dependency] {
			log.Printf("Missing dependency %s for plugin %s", dependency, pluginName)
			return false
		}
	}

	log.Printf("All dependencies satisfied for plugin %s", pluginName)
	return true
}

// filterDependenciesByAnalysis filters the dependency list to only include plugins 
// that are actually present in the current analysis
func (dr *DependencyResolver) filterDependenciesByAnalysis(dependencies []string, analysis *codeclarity.Analysis) []string {
	// Build a set of all plugins present in this analysis
	analysisPlugins := make(map[string]bool)
	for _, stage := range analysis.Steps {
		for _, step := range stage {
			analysisPlugins[step.Name] = true
		}
	}
	
	// Filter dependencies to only include those present in the analysis
	var actualDependencies []string
	for _, dependency := range dependencies {
		if analysisPlugins[dependency] {
			actualDependencies = append(actualDependencies, dependency)
		} else {
			log.Printf("Dependency %s not present in current analysis, skipping", dependency)
		}
	}
	
	return actualDependencies
}

// HasPendingDependentPlugins checks if there are plugins waiting for dependencies in any stage
func (dr *DependencyResolver) HasPendingDependentPlugins(analysis *codeclarity.Analysis) bool {
	for stageIndex, stage := range analysis.Steps {
		for _, step := range stage {
			if step.Status == "" {
				// Check if this plugin has dependencies that might now be satisfied
				if pluginConfig, exists := dr.pluginConfigs[step.Name]; exists && len(pluginConfig.DependsOn) > 0 {
					if dr.areDependenciesSatisfied(step.Name, analysis) {
						log.Printf("Found plugin %s in stage %d that is now ready to run", step.Name, stageIndex)
						return true
					}
				}
			}
		}
	}
	return false
}

// TopologicalSort organizes plugins within a stage based on dependencies
func (dr *DependencyResolver) TopologicalSort(plugins []codeclarity.Step) []codeclarity.Step {
	// Simple topological sort implementation
	var sorted []codeclarity.Step
	processed := make(map[string]bool)

	// Keep adding plugins whose dependencies are in the processed set
	for len(sorted) < len(plugins) {
		addedInThisRound := false
		
		for _, plugin := range plugins {
			if processed[plugin.Name] {
				continue
			}

			// Check if all dependencies are already processed or have no dependencies
			canAdd := true
			if pluginConfig, exists := dr.pluginConfigs[plugin.Name]; exists {
				for _, dep := range pluginConfig.DependsOn {
					// Only check dependencies within this stage
					depInThisStage := false
					for _, otherPlugin := range plugins {
						if otherPlugin.Name == dep {
							depInThisStage = true
							break
						}
					}
					
					if depInThisStage && !processed[dep] {
						canAdd = false
						break
					}
				}
			}

			if canAdd {
				sorted = append(sorted, plugin)
				processed[plugin.Name] = true
				addedInThisRound = true
			}
		}

		// If we couldn't add any plugins in this round, we have a circular dependency
		if !addedInThisRound {
			log.Printf("Warning: Possible circular dependency detected, adding remaining plugins")
			for _, plugin := range plugins {
				if !processed[plugin.Name] {
					sorted = append(sorted, plugin)
					processed[plugin.Name] = true
				}
			}
		}
	}

	return sorted
}