package model

import (
	"encoding/json"
	"testing"
)

func TestCellsetCellJSON(t *testing.T) {
	tests := []struct {
		name      string
		jsonInput string
		wantValue interface{}
	}{
		{
			name:      "numeric value",
			jsonInput: `{"Value":42.5,"Ordinal":0}`,
			wantValue: 42.5,
		},
		{
			name:      "string value",
			jsonInput: `{"Value":"hello","Ordinal":1}`,
			wantValue: "hello",
		},
		{
			name:      "null value",
			jsonInput: `{"Value":null,"Ordinal":2}`,
			wantValue: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cell CellsetCell
			if err := json.Unmarshal([]byte(tt.jsonInput), &cell); err != nil {
				t.Fatalf("Unmarshal error: %v", err)
			}

			if tt.wantValue == nil {
				if cell.Value != nil {
					t.Errorf("Value = %v, want nil", cell.Value)
				}
			} else {
				switch want := tt.wantValue.(type) {
				case float64:
					got, ok := cell.Value.(float64)
					if !ok || got != want {
						t.Errorf("Value = %v, want %v", cell.Value, want)
					}
				case string:
					got, ok := cell.Value.(string)
					if !ok || got != want {
						t.Errorf("Value = %v, want %v", cell.Value, want)
					}
				default:
					t.Errorf("unexpected wantValue type %T", tt.wantValue)
				}
			}
		})
	}
}

func TestCellsetMemberJSON(t *testing.T) {
	m := CellsetMember{Name: "Q1"}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	want := `{"Name":"Q1"}`
	if string(data) != want {
		t.Errorf("Marshal = %s, want %s", data, want)
	}

	var got CellsetMember
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if got != m {
		t.Errorf("Round-trip = %+v, want %+v", got, m)
	}
}

func TestCellsetMemberUniqueNameJSON(t *testing.T) {
	m := CellsetMember{Name: "Jan", UniqueName: "[Period].[Period].[Jan]"}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	want := `{"Name":"Jan","UniqueName":"[Period].[Period].[Jan]"}`
	if string(data) != want {
		t.Errorf("Marshal = %s, want %s", data, want)
	}

	var got CellsetMember
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if got != m {
		t.Errorf("Round-trip = %+v, want %+v", got, m)
	}
}

func TestCellsetTupleJSON(t *testing.T) {
	tuple := CellsetTuple{
		Ordinal: 0,
		Members: []CellsetMember{{Name: "USA"}, {Name: "Q1"}},
	}

	data, err := json.Marshal(tuple)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var got CellsetTuple
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if got.Ordinal != tuple.Ordinal {
		t.Errorf("Ordinal = %d, want %d", got.Ordinal, tuple.Ordinal)
	}
	if len(got.Members) != 2 {
		t.Fatalf("got %d members, want 2", len(got.Members))
	}
	if got.Members[0].Name != "USA" || got.Members[1].Name != "Q1" {
		t.Errorf("Members = %+v, want [USA, Q1]", got.Members)
	}
}

func TestCellsetAxisJSON(t *testing.T) {
	axis := CellsetAxis{
		Ordinal: 1,
		Tuples: []CellsetTuple{
			{Ordinal: 0, Members: []CellsetMember{{Name: "USA"}}},
			{Ordinal: 1, Members: []CellsetMember{{Name: "Canada"}}},
		},
	}

	data, err := json.Marshal(axis)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var got CellsetAxis
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if got.Ordinal != 1 {
		t.Errorf("Ordinal = %d, want 1", got.Ordinal)
	}
	if len(got.Tuples) != 2 {
		t.Fatalf("got %d tuples, want 2", len(got.Tuples))
	}
}

func TestCellsetResponseJSON(t *testing.T) {
	jsonInput := `{
		"Axes": [
			{
				"Ordinal": 0,
				"Tuples": [
					{"Ordinal": 0, "Members": [{"Name": "Jan"}]},
					{"Ordinal": 1, "Members": [{"Name": "Feb"}]}
				]
			},
			{
				"Ordinal": 1,
				"Tuples": [
					{"Ordinal": 0, "Members": [{"Name": "USA"}]}
				]
			}
		],
		"Cells": [
			{"Value": 100.5, "Ordinal": 0},
			{"Value": null, "Ordinal": 1}
		]
	}`

	var resp CellsetResponse
	if err := json.Unmarshal([]byte(jsonInput), &resp); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if len(resp.Axes) != 2 {
		t.Fatalf("got %d axes, want 2", len(resp.Axes))
	}
	if len(resp.Axes[0].Tuples) != 2 {
		t.Errorf("axis 0 tuples = %d, want 2", len(resp.Axes[0].Tuples))
	}
	if len(resp.Axes[1].Tuples) != 1 {
		t.Errorf("axis 1 tuples = %d, want 1", len(resp.Axes[1].Tuples))
	}
	if len(resp.Cells) != 2 {
		t.Fatalf("got %d cells, want 2", len(resp.Cells))
	}

	val, ok := resp.Cells[0].Value.(float64)
	if !ok || val != 100.5 {
		t.Errorf("cell 0 value = %v, want 100.5", resp.Cells[0].Value)
	}
	if resp.Cells[1].Value != nil {
		t.Errorf("cell 1 value = %v, want nil", resp.Cells[1].Value)
	}
}
