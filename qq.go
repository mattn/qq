package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode"

	"github.com/mattn/go-encoding"
	"github.com/mattn/go-runewidth"
	_ "github.com/mattn/go-sqlite3"
)

var (
	noheader  = flag.Bool("nh", false, "don't treat first line as header")
	outheader = flag.Bool("oh", false, "output header line")
	inputcsv  = flag.Bool("ic", false, "input csv")
	inputtsv  = flag.Bool("it", false, "input tsv")
	inputpat  = flag.String("ip", "", "input pattern as regexp")
	enc       = flag.String("e", "", "encoding of input stream")
	query     = flag.String("q", "", "select query")
)

func readLines(r io.Reader) ([]string, error) {
	var lines []string
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		lines = append(lines, line)
	}
	return lines, scanner.Err()
}

func lines2rows(lines []string) [][]string {
	cr := []rune(lines[0])
	w := 0

	rows := make([][]string, len(lines))
	i := 0

skip_white:
	for ; i < len(cr); i++ {
		for _, line := range lines {
			if !unicode.IsSpace(rune(line[i])) {
				break skip_white
			}
		}
		w++
	}
	li := i

	for ; i < len(cr); i++ {
		r := cr[i]
		w += runewidth.RuneWidth(r)
		last := i == len(cr)-1

		if i == 0 || (!unicode.IsSpace(r) && !last) {
			continue
		}

		if last {
			for ri := range rows {
				fv := strings.TrimSpace(string(([]rune(lines[ri]))[li:]))
				if ri == 0 && fv == "" && !*noheader {
					fv = fmt.Sprintf("______f%d", ri+1)
				}
				rows[ri] = append(rows[ri], fv)
			}
		} else {
			part := false
			for _, line := range lines {
				pr := []rune(runewidth.Truncate(line, w, ""))
				if !unicode.IsSpace(pr[len(pr)-1]) {
					part = true
					break
				}
			}
			if !part {
				for ri := range rows {
					lr := []rune(lines[ri])
					ib := i
					if ib >= len(lr) {
						ib = len(lr) - 1
					}
					fv := strings.TrimSpace(string(lr[li:ib]))
					if ri == 0 && fv == "" && !*noheader {
						fv = fmt.Sprintf("______f%d", ri+1)
					}
					rows[ri] = append(rows[ri], fv)
				}

				for ; i < len(cr); i++ {
					cw := runewidth.RuneWidth(r)
					for _, line := range lines {
						pr := []rune(runewidth.Truncate(line, w+cw, ""))
						if !unicode.IsSpace(pr[len(pr)-1]) {
							part = true
							break
						}
					}
					if part {
						break
					}
					w += cw
				}

				li = i
			}
		}
	}
	return rows
}

func qq(stdin io.Reader) ([][]string, error) {
	var rows [][]string
	var err error

	if *inputcsv {
		rows, err = csv.NewReader(stdin).ReadAll()
		if err != nil {
			return nil, err
		}
	} else if *inputtsv {
		csv := csv.NewReader(stdin)
		csv.Comma = '\t'
		rows, err = csv.ReadAll()
		if err != nil {
			return nil, err
		}
	} else if *inputpat != "" {
		lines, err := readLines(stdin)
		if err != nil {
			return nil, err
		}
		if len(lines) == 0 {
			return nil, nil
		}
		re, err := regexp.Compile(*inputpat)
		if err != nil {
			return nil, err
		}
		for _, line := range lines {
			rows = append(rows, re.Split(line, -1))
		}
	} else {
		lines, err := readLines(stdin)
		if err != nil {
			return nil, err
		}
		if len(lines) == 0 {
			return nil, nil
		}
		rows = lines2rows(lines)
	}

	if *query != "" {
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return nil, err
		}
		defer db.Close()

		var cn []string
		if *noheader {
			for i := 0; i < len(rows[0]); i++ {
				cn = append(cn, fmt.Sprintf(`f%d`, i+1))
			}
		} else {
			cn = rows[0]
		}
		s := `create table "stdin"(`
		for i, n := range cn {
			if i > 0 {
				s += `,`
			}
			s += `'` + strings.Replace(n, `'`, `\'`, -1) + `'`
		}
		s += `)`
		_, err = db.Exec(s)
		if err != nil {
			return nil, err
		}

		s = `insert into "stdin"(`
		for i, n := range cn {
			if i > 0 {
				s += `,`
			}
			s += `'` + strings.Replace(n, `'`, `\'`, -1) + `'`
		}
		s += `) values`
		d := ``
		for r, row := range rows {
			if r == 0 && !*noheader {
				continue
			}
			if d != `` {
				d += `,`
			}
			d += `(`
			for i, col := range row {
				if i > 0 {
					d += `,`
				}
				d += `'` + strings.Replace(col, `'`, `\'`, -1) + `'`
			}
			d += `)`
		}
		_, err = db.Exec(s + d)
		if err != nil {
			return nil, err
		}

		qrows, err := db.Query(*query)
		if err != nil {
			return nil, err
		}
		defer qrows.Close()

		cols, err := qrows.Columns()
		if err != nil {
			return nil, err
		}

		rows = [][]string{}
		if *outheader {
			rows = append(rows, cols)
		}

		values := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i, _ := range cols {
			ptrs[i] = &values[i]
		}
		for qrows.Next() {
			err = qrows.Scan(ptrs...)
			if err != nil {
				return nil, err
			}

			cells := []string{}
			for _, val := range values {
				b, ok := val.([]byte)
				var v string
				if ok {
					v = string(b)
				} else {
					v = fmt.Sprint(val)
				}
				cells = append(cells, v)
			}
			rows = append(rows, cells)
		}
	}

	return rows, nil
}

func main() {
	flag.Parse()

	var stdin io.Reader = os.Stdin
	if *enc != "" {
		ee := encoding.GetEncoding(*enc)
		if ee == nil {
			fmt.Fprintln(os.Stderr, "invalid encoding name:", *enc)
			os.Exit(1)
		}
		stdin = ee.NewDecoder().Reader(stdin)
	}

	rows, err := qq(stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	err = csv.NewWriter(os.Stdout).WriteAll(rows)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
