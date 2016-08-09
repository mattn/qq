package qq

import (
	"io"
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
	qq, err := NewQQ(&Option{})
	if err != nil {
		t.Fatal(err)
	}
	defer qq.Close()

	for _, testcase := range testcases_lines2rows {
		rows := qq.lines2rows(testcase.input)
		if !reflect.DeepEqual(rows, testcase.output) {
			t.Fatalf("%q should be parsed as %v: got %v", testcase.input, testcase.output, rows)
		}
	}
}

func test(r io.Reader, name string, query string, opt *Option) ([][]string, error) {
	qq, err := NewQQ(opt)
	if err != nil {
		return nil, err
	}
	defer qq.Close()

	err = qq.Import(r, "stdin")
	if err != nil {
		return nil, err
	}

	rows, err := qq.Query(query)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func TestQQ(t *testing.T) {
	input := `
PID command
  1 /usr/bin/ls
  2 /usr/bin/grep
  3 /usr/bin/php run.php --opt='1'
`
	rows, err := test(strings.NewReader(input), "stdin", "select pid from stdin", &Option{})
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 3 {
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

	if rows[2][0] != "3" {
		t.Fatalf("second result should be 3: got %v", rows[0][0])
	}

	rows, err = test(strings.NewReader(input), "stdin", "select command from stdin where pid = 2", &Option{})
	if err != nil {
		t.Fatal(err)
	}

	if rows[0][0] != "/usr/bin/grep" {
		t.Fatalf("result should be '/usr/bin/grep': got %v", rows[0][0])
	}

	rows, err = test(strings.NewReader(input), "stdin", "select command from stdin where pid = 3", &Option{})
	if err != nil {
		t.Fatal(err)
	}

	if rows[0][0] != "/usr/bin/php run.php --opt='1'" {
		t.Fatalf("result should be '/usr/bin/php run.php --opt='1': got %v", rows[0][0])
	}
}

func TestInputCSV(t *testing.T) {
	input := `
PID,command
1,/usr/bin/ls
2,/usr/bin/grep
`
	opt := &Option{
		InputCSV: true,
	}
	rows, err := test(strings.NewReader(input), "stdin", "select pid from stdin", opt)
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

	rows, err = test(strings.NewReader(input), "stdin", "select command from stdin where pid = 2", opt)
	if err != nil {
		t.Fatal(err)
	}

	if rows[0][0] != "/usr/bin/grep" {
		t.Fatalf("result should be '/usr/bin/grep': got %v", rows[0][0])
	}
}

func TestInputTSV(t *testing.T) {
	input := "PID\tcommand\n1\t/usr/bin/ls\n2\t/usr/bin/grep\n"

	opt := &Option{
		InputTSV: true,
	}
	rows, err := test(strings.NewReader(input), "stdin", "select pid from stdin", opt)
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

	rows, err = test(strings.NewReader(input), "stdin", "select command from stdin where pid = 2", opt)
	if err != nil {
		t.Fatal(err)
	}

	if rows[0][0] != "/usr/bin/grep" {
		t.Fatalf("result should be '/usr/bin/grep': got %v", rows[0][0])
	}
}

func TestInputPat(t *testing.T) {
	input := "PID#command\n1#/usr/bin/ls\n2#/usr/bin/grep\n"

	opt := &Option{
		InputPat: `#`,
	}
	rows, err := test(strings.NewReader(input), "stdin", "select pid from stdin", opt)
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

	rows, err = test(strings.NewReader(input), "stdin", "select command from stdin where pid = 2", opt)
	if err != nil {
		t.Fatal(err)
	}

	if rows[0][0] != "/usr/bin/grep" {
		t.Fatalf("result should be '/usr/bin/grep': got %v", rows[0][0])
	}
}
