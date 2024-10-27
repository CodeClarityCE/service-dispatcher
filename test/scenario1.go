package test

import (
	"encoding/json"

	types_amqp "github.com/CodeClarityCE/utility-types/amqp"
)

func Scenario1(queue_name string) error {
	// Create mock data
	res := types_amqp.SymfonyDispatcherContext{
		Context: types_amqp.SymfonyDispatcherMessage{
			Analysis:           0,
			Analyzers:          []string{"JS"},
			Date:               types_amqp.Date{Date: "", TimezoneType: ""},
			Uid:                0,
			Project:            "",
			DisallowedLicenses: []string{},
		},
	}
	// data, _ := json.Marshal(res)
	_, _ = json.Marshal(res)

	// SendMessage(queue_name, data)

	return nil
}
