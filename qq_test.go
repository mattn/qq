package main

import (
	"reflect"
	"testing"
)

var testcases = []struct {
	input  []string
	output [][]string
}{
	{
		input: []string{
			"  PID command   ",
			"   1   ls       ",
		},
		output: [][]string{
			[]string{"PID", "command"}, {"1", "ls"},
		},
	},
	{
		input: []string{
			"  PID command   ",
			"     1   ls     ",
		},
		output: [][]string{
			[]string{"PID command"}, {"1   ls"},
		},
	},
	{
		input: []string{
			"  PID command   ",
			"      1   ls    ",
		},
		output: [][]string{
			[]string{"PID", "command"}, {"", "1   ls"},
		},
	},
	{
		input: []string{
			"      command   ",
			"    1   ls      ",
		},
		output: [][]string{
			[]string{"______f1", "command"}, {"1", "ls"},
		},
	},
	{
		input: []string{
			" 1 ",
			"  ",
		},
		output: [][]string{
			[]string{"1"}, {""},
		},
	},
	{
		input: []string{
			"   ",
			" 1 ",
		},
		output: [][]string{
			[]string{"______f1"}, {"1"},
		},
	},
}

func TestLines2Rows(t *testing.T) {
	for _, testcase := range testcases {
		rows := lines2rows(testcase.input)
		if !reflect.DeepEqual(rows, testcase.output) {
			t.Fatalf("%q should be parsed as %v: got %v", testcase.input, testcase.output, rows)
		}
	}
}
