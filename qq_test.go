package main

import (
	"reflect"
	"strings"
	"testing"
)

var testcases_readlines = []struct {
	input  string
	output []string
}{
	{
		input: "  PID command   \n" +
			"\n" +
			"   1   ls       \n",
		output: []string{
			"  PID command   ", "   1   ls       ",
		},
	},
}

func TestReadLines(t *testing.T) {
	for _, testcase := range testcases_readlines {
		lines, err := readLines(strings.NewReader(testcase.input))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(lines, testcase.output) {
			t.Fatalf("%q should be read as %v: got %v", testcase.input, testcase.output, lines)
		}
	}
}

var testcases_lines2rows = []struct {
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
	{
		input: []string{
			"a b",
			"1 ",
		},
		output: [][]string{
			[]string{"a", "b"}, {"1", ""},
		},
	},
}

func TestLines2Rows(t *testing.T) {
	for _, testcase := range testcases_lines2rows {
		rows := lines2rows(testcase.input)
		if !reflect.DeepEqual(rows, testcase.output) {
			t.Fatalf("%q should be parsed as %v: got %v", testcase.input, testcase.output, rows)
		}
	}
}
