# qq

[![Build Status](https://travis-ci.org/mattn/qq.svg?branch=master)](https://travis-ci.org/mattn/qq)

[![Coverage Status](https://coveralls.io/repos/github/mattn/qq/badge.svg?branch=master)](https://coveralls.io/github/mattn/qq?branch=master)

Select stdin with query.

## Usage

```
$ ps | qq -q "select pid from stdin"
9324
16344
13824
```

## Requirements

* go

## Installation

```
$ go get github.com/mattn/qq
```

## License

MIT

## Author

Yasuhiro Matsumoto (a.k.a. mattn)
