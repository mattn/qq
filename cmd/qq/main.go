package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mattn/go-encoding"
	"github.com/mattn/qq"
	xenc "golang.org/x/text/encoding"
)

var (
	noheader   = flag.Bool("nh", false, "don't treat first line as header")
	outheader  = flag.Bool("oh", false, "output header line")
	inputcsv   = flag.Bool("ic", false, "input csv")
	inputtsv   = flag.Bool("it", false, "input tsv")
	inputltsv  = flag.Bool("il", false, "input ltsv")
	inputpat   = flag.String("ip", "", "input delimiter pattern as regexp")
	outputjson = flag.Bool("oj", false, "output json")
	outputraw  = flag.Bool("or", false, "output raw")
	enc        = flag.String("e", "", "encoding of input stream")
	query      = flag.String("q", "", "select query")
)

func main() {
	flag.Parse()

	var ee xenc.Encoding
	if *enc != "" {
		ee = encoding.GetEncoding(*enc)
		if ee == nil {
			fmt.Fprintln(os.Stderr, "invalid encoding name:", *enc)
			os.Exit(1)
		}
	}

	qq, err := qq.NewQQ(&qq.Option{
		NoHeader:  *noheader,
		OutHeader: *outheader,
		InputCSV:  *inputcsv,
		InputTSV:  *inputtsv,
		InputLTSV: *inputltsv,
		InputPat:  *inputpat,
		Encoding:  ee,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	err = qq.Import(os.Stdin, "stdin")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	for _, fn := range flag.Args() {
		if fn != "-" {
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

	if *query == "" {
		*query = "select * from stdin"
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
