package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
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
	enc       = flag.String("e", "", "encoding of input stream")
	query     = flag.String("q", "", "select query")
)

func fatalIf(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
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
				lr := []rune(lines[ri])
				rows[ri] = append(rows[ri], strings.TrimSpace(string(lr[li:])))
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
					rows[ri] = append(rows[ri], strings.TrimSpace(string(lr[li:ib])))
				}

				for ; i < len(cr); i++ {
					cr := runewidth.RuneWidth(r)
					for _, line := range lines {
						pr := []rune(runewidth.Truncate(line, w+cr, ""))
						if !unicode.IsSpace(pr[len(pr)-1]) {
							part = true
							break
						}
					}
					if part {
						break
					}
					w += cr
				}

				li = i
			}
		}
	}
	return rows
}

func main() {
	flag.Parse()

	var stdin io.Reader = os.Stdin
	if *enc != "" {
		ee := encoding.GetEncoding(*enc)
		if ee == nil {
			fatalIf(fmt.Errorf("invalid encoding name:", *enc))
		}
		stdin = ee.NewDecoder().Reader(stdin)
	}

	var rows [][]string
	var err error

	if *inputcsv {
		rows, err = csv.NewReader(os.Stdin).ReadAll()
		fatalIf(err)
	} else {
		b, err := ioutil.ReadAll(stdin)
		fatalIf(err)

		lines := []string{}
		for _, line := range strings.Split(string(b), "\n") {
			if strings.TrimSpace(line) != "" {
				lines = append(lines, line)
			}
		}
		if len(lines) == 0 {
			return
		}
		rows = lines2rows(lines)
	}

	if *query != "" {
		db, err := sql.Open("sqlite3", ":memory:")
		fatalIf(err)
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
		fatalIf(err)

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
		fatalIf(err)

		qrows, err := db.Query(*query)
		fatalIf(err)
		defer qrows.Close()

		cols, err := qrows.Columns()
		fatalIf(err)

		rows = [][]string{}

		values := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i, _ := range cols {
			ptrs[i] = &values[i]
		}
		for qrows.Next() {
			err = qrows.Scan(ptrs...)
			fatalIf(err)

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

	csv.NewWriter(os.Stdout).WriteAll(rows)
}
