package main

import (
	"os"
	"testing"
)

// func TestReceiveSymfony(t *testing.T) {
// 	var tests = []struct {
// 		value string
// 		want  error
// 	}{
// 		{"symfony_dispatcher", nil},
// 		{"sbom_dispatcher", nil},
// 	}
// 	for _, tt := range tests {
// 		testname := fmt.Sprintf(tt.value)
// 		t.Run(testname, func(t *testing.T) {
// 			// START TEST
// 			_, msg, err := openConnection(tt.value)
// 			if err != nil {
// 				t.Errorf(msg)
// 			}
// 			// END TEST
// 		})
// 	}
// }

func TestScenario1(t *testing.T) {
	// Integration test: it starts the real dispatcher service, which needs
	// Postgres (PG_DB_*) and a RabbitMQ broker. CI provides neither.
	if os.Getenv("PG_DB_HOST") == "" {
		t.Skip("integration test: set PG_DB_* and run a RabbitMQ broker to enable")
	}
	// Send mock data from scenario 1
	Dispatcher()
}
