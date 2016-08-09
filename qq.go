package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"

	xenc "golang.org/x/text/encoding"

	"github.com/mattn/go-encoding"
	"github.com/mattn/go-runewidth"
	_ "github.com/mattn/go-sqlite3"
)

var (
	noheader   = flag.Bool("nh", false, "don't treat first line as header")
	outheader  = flag.Bool("oh", false, "output header line")
	inputcsv   = flag.Bool("ic", false, "input csv")
	inputtsv   = flag.Bool("it", false, "input tsv")
	inputpat   = flag.String("ip", "", "input delimiter pattern as regexp")
	outputjson = flag.Bool("oj", false, "output json")
	outputraw  = flag.Bool("or", false, "output raw")
	enc        = flag.String("e", "", "encoding of input stream")
	query      = flag.String("q", "", "select query")

	renum = regexp.MustCompile(`^[+-]?[1-9][0-9]*(\.[0-9]+)?(e-?[0-9]+)?$`)
	ee    xenc.Encoding
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

type QQ struct {
	db *sql.DB
}

func NewQQ() (*QQ, error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}
	return &QQ{db}, nil
}

func (qq *QQ) Import(r io.Reader, name string) error {
	var rows [][]string
	var err error

	if ee != nil {
		r = ee.NewDecoder().Reader(r)
	}

	if *inputcsv {
		rows, err = csv.NewReader(r).ReadAll()
		if err != nil {
			return err
		}
	} else if *inputtsv {
		csv := csv.NewReader(r)
		csv.Comma = '\t'
		rows, err = csv.ReadAll()
		if err != nil {
			return err
		}
	} else if *inputpat != "" {
		lines, err := readLines(r)
		if err != nil {
			return err
		}
		if len(lines) == 0 {
			return nil
		}
		re, err := regexp.Compile(*inputpat)
		if err != nil {
			return err
		}
		for _, line := range lines {
			rows = append(rows, re.Split(line, -1))
		}
	} else {
		lines, err := readLines(r)
		if err != nil {
			return err
		}
		if len(lines) == 0 {
			return nil
		}
		rows = lines2rows(lines)
	}

	var cn []string
	if *noheader {
		for i := 0; i < len(rows[0]); i++ {
			cn = append(cn, fmt.Sprintf(`f%d`, i+1))
		}
	} else {
		cn = rows[0]
	}
	s := `create table '` + strings.Replace(name, `'`, `''`, -1) + `'(`
	for i, n := range cn {
		if i > 0 {
			s += `,`
		}
		s += `'` + strings.Replace(n, `'`, `''`, -1) + `'`
	}
	s += `)`
	_, err = qq.db.Exec(s)
	if err != nil {
		return err
	}

	s = `insert into '` + strings.Replace(name, `'`, `''`, -1) + `'(`
	for i, n := range cn {
		if i > 0 {
			s += `,`
		}
		s += `'` + strings.Replace(n, `'`, `''`, -1) + `'`
	}
	s += `) values`
	d := ``
	for rid, row := range rows {
		if rid == 0 && !*noheader {
			continue
		}
		if d != `` {
			d += `,`
		}
		d += `(`
		for i, col := range row {
			if i >= len(cn) {
				break
			}
			if i > 0 {
				d += `,`
			}
			if renum.MatchString(col) {
				d += col
			} else {
				d += `'` + strings.Replace(col, `'`, `''`, -1) + `'`
			}
		}
		d += `)`
	}
	_, err = qq.db.Exec(s + d)
	if err != nil {
		return err
	}
	return nil
}

func (qq *QQ) Query(query string) ([][]string, error) {
	qrows, err := qq.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer qrows.Close()

	cols, err := qrows.Columns()
	if err != nil {
		return nil, err
	}

	rows := [][]string{}
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

	return rows, nil
}

func (qq *QQ) Close() error {
	return qq.db.Close()
}

func main() {
	flag.Parse()

	if *query == "" {
		flag.Usage()
		os.Exit(1)
	}

	qq, err := NewQQ()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if *enc != "" {
		ee = encoding.GetEncoding(*enc)
		if ee == nil {
			fmt.Fprintln(os.Stderr, "invalid encoding name:", *enc)
			os.Exit(1)
		}
	}

	for _, fn := range flag.Args() {
		if fn == "-" {
			err = qq.Import(os.Stdin, "stdin")
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		} else {
			fb := filepath.Base(fn)
			f, err := os.Open(fn)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			err = qq.Import(f, fb)
			f.Close()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
	}

	rows, err := qq.Query(*query)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if *outputjson {
		err = json.NewEncoder(os.Stdout).Encode(rows)
	} else if *outputraw {
		for _, row := range rows {
			for c, col := range row {
				if c > 0 {
					fmt.Print("\t")
				}
				fmt.Print(col)
			}
			fmt.Println()
		}
	} else {
		err = csv.NewWriter(os.Stdout).WriteAll(rows)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
