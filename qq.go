package qq

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"regexp"
	"strings"
	"unicode"

	xenc "golang.org/x/text/encoding"

	"github.com/mattn/go-runewidth"
	_ "github.com/mattn/go-sqlite3"
	"github.com/najeira/ltsv"
)

const (
	Comma = ","
)

// QQ is the most basic structure of qq
type QQ struct {
	db  *sql.DB
	Opt *Option
}

// Option is a structure that qq command can receive
type Option struct {
	NoHeader  bool
	OutHeader bool
	InputCSV  bool
	InputTSV  bool
	InputLTSV bool
	InputPat  string
	Encoding  xenc.Encoding
}

var renum = regexp.MustCompile(`^[+-]?[1-9][0-9]*(\.[0-9]+)?(e-?[0-9]+)?$`)

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

func (qq *QQ) lines2rows(lines []string) [][]string {
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
				if ri == 0 && fv == "" && !qq.Opt.NoHeader {
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
					if ri == 0 && fv == "" && !qq.Opt.NoHeader {
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

// NewQQ creates new connection to sqlite3
func NewQQ(opt *Option) (*QQ, error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}
	return &QQ{db, opt}, nil
}

const (
	sqliteINTEGER = "INTEGER"
	sqliteTEXT    = "TEXT"
	sqliteREAL    = "REAL"
)

type column struct {
	Name string
	Type string
}

func newColumn(name string) *column {
	return &column{
		Name: name,
		Type: sqliteINTEGER,
	}
}

func (qq *QQ) columnsAndRows(r io.Reader) (cn []*column, rows [][]string, err error) {
	if qq.Opt.Encoding != nil {
		r = qq.Opt.Encoding.NewDecoder().Reader(r)
	}
	if qq.Opt.InputCSV {
		rows, err = csv.NewReader(r).ReadAll()
		if err != nil {
			return nil, nil, err
		}
	} else if qq.Opt.InputTSV {
		csv := csv.NewReader(r)
		csv.Comma = '\t'
		rows, err = csv.ReadAll()
		if err != nil {
			return nil, nil, err
		}
	} else if qq.Opt.InputLTSV {
		rawRows, err := ltsv.NewReader(r).ReadAll()
		if err != nil {
			return nil, nil, err
		}
		keys := make(map[string]struct{})
		for _, rowMap := range rawRows {
			for k := range rowMap {
				keys[k] = struct{}{}
			}
		}
		for k := range keys {
			cn = append(cn, newColumn(k))
		}
		for _, rowMap := range rawRows {
			row := make([]string, len(cn))
			for i, v := range cn {
				row[i] = rowMap[v.Name]
			}
			rows = append(rows, row)
		}
	} else if qq.Opt.InputPat != "" {
		lines, err := readLines(r)
		if err != nil {
			return nil, nil, err
		}
		if len(lines) == 0 {
			return nil, nil, nil
		}
		re, err := regexp.Compile(qq.Opt.InputPat)
		if err != nil {
			return nil, nil, err
		}
		for _, line := range lines {
			rows = append(rows, re.Split(line, -1))
		}
	} else {
		lines, err := readLines(r)
		if err != nil {
			return nil, nil, err
		}
		if len(lines) == 0 {
			return nil, nil, nil
		}
		rows = qq.lines2rows(lines)
	}

	if !qq.Opt.InputLTSV {
		if qq.Opt.NoHeader {
			for i := 0; i < len(rows[0]); i++ {
				cn = append(cn, newColumn(fmt.Sprintf(`f%d`, i+1)))
			}
		} else {
			for _, v := range rows[0] {
				cn = append(cn, newColumn(v))
			}
		}
	}
	if !qq.Opt.NoHeader && !qq.Opt.InputLTSV {
		rows = rows[1:]
	}
	for _, row := range rows {
		for i, col := range row {
			if col == "" {
				continue
			}
			colDef := cn[i]
			if colDef.Type == sqliteTEXT {
				continue
			}
			if matches := renum.FindStringSubmatch(col); len(matches) > 0 {
				if matches[1] != "" || matches[2] != "" {
					colDef.Type = sqliteREAL
				}
			} else {
				colDef.Type = sqliteTEXT
			}
		}
	}
	return cn, rows, nil
}

// Import from csv/tsv files or stdin
func (qq *QQ) Import(r io.Reader, name string) error {
	cn, rows, err := qq.columnsAndRows(r)
	if err != nil || len(rows) == 0 {
		return err
	}
	s := `create table '` + strings.Replace(name, `'`, `''`, -1) + `'(`
	for i, n := range cn {
		if i > 0 {
			s += Comma
		}
		s += fmt.Sprintf(`'%s' %s`, strings.Replace(n.Name, `'`, `''`, -1), n.Type)
	}
	s += `)`
	_, err = qq.db.Exec(s)
	if err != nil {
		return err
	}

	s = `insert into '` + strings.Replace(name, `'`, `''`, -1) + `'(`
	for i, n := range cn {
		if i > 0 {
			s += Comma
		}
		s += `'` + strings.Replace(n.Name, `'`, `''`, -1) + `'`
	}
	s += `) values`
	d := ``
	for _, row := range rows {
		if d != `` {
			d += `,`
		}
		d += `(`
		for i, col := range row {
			if i >= len(cn) {
				break
			}
			if i > 0 {
				d += Comma
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

// Query runs a query and formatize result set
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
	if len(cols) == 0 {
		return nil, nil
	}

	rows := [][]string{}
	if qq.Opt.OutHeader {
		rows = append(rows, cols)
	}

	values := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range cols {
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

// Close database connection
func (qq *QQ) Close() error {
	return qq.db.Close()
}
