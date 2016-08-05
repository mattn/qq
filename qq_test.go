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

func TestQQ(t *testing.T) {
	input := `
PID command
  1 /usr/bin/ls
  2 /usr/bin/grep
`
	*query = "select pid from stdin"
	rows, err := qq(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 2 {
		t.Fatalf("rows should have two row: got %v", rows)
	}

	if len(rows[0]) != 1 {
		t.Fatalf("columns should have only one: got %v", rows[0])
	}

	if rows[0][0] != "1" {
		t.Fatalf("first result should be 1: got %v", rows[0][0])
	}

	if rows[1][0] != "2" {
		t.Fatalf("second result should be 2: got %v", rows[0][0])
	}

	*query = "select command from stdin where pid = '2'"
	rows, err = qq(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}

	if rows[0][0] != "/usr/bin/grep" {
		t.Fatalf("result should be '/usr/bin/grep': got %v", rows[0][0])
	}
}
