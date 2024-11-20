package sql

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Lexer struct {
	input          string
	pos            int
	ParseTree      Statement
	partialDDL     *DDL
	lastError      error
	lastToken      []byte
	lastChar       rune
	remainingInput string
}

func NewLexer(input string) *Lexer {
	return &Lexer{input: input, remainingInput: input}
}
func (l *Lexer) Lex(lval *yySymType) int {
	// Skip any leading whitespace
	for l.pos < len(l.input) && isWhitespace(rune(l.input[l.pos])) {
		l.pos++
	}

	if l.pos >= len(l.input) {
		return 0 // EOF
	}

	id, val := l.Scan(lval)
	lval.str = val
	l.lastToken = []byte(val)
	l.remainingInput = l.input[l.pos:]
	if !l.eof(l.pos) {
		l.lastChar = rune(l.remainingInput[0])
	}
	return id
}

func (l *Lexer) Scan(lval *yySymType) (int, string) {
	switch ch := l.input[l.pos]; {
	case isLetter(rune(ch)):
		return l.scanToken(l.pos)
	default:
		switch ch {

		case '*':
			l.pos++
			return STAR, "*"
		case '(':
			l.pos++
			return LPAREN, "("
		case ')':
			l.pos++
			return RPAREN, ")"

		// reserved token
		case '=', ',', ';', '+', '%', '^', '~':
			token, ok := tokens[string(ch)]
			if !ok {
				return 0, strconv.Itoa(token)
			}
			l.pos++
			l.lastToken = []byte(string(ch))
			return token, string(l.lastToken)

		case '.':
			if isDigit(rune(ch)) {
				//return tkn.scanNumber(true)
			}
			l.pos++
			return int(ch), "."
		}
	}
	return -1, ""
}

func (l *Lexer) Error(err string) {
	buf := &bytes.Buffer{}
	if l.lastToken != nil {
		fmt.Fprintf(buf, "%s at position %v near '%s'", err, l.pos, l.lastToken)
	} else {
		fmt.Fprintf(buf, "%s at position %v", err, l.pos)
	}
	l.lastError = errors.New(buf.String())
}

func (l *Lexer) eof(pos int) bool {
	return pos >= len(l.input)
}
func isWhitespace(c rune) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}
func isLetter(ch rune) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_' || ch == '@'
}

func isDigit(ch rune) bool {
	return '0' <= ch && ch <= '9'
}

func (l *Lexer) scanToken(pos int) (int, string) {
	ch := l.input[pos]
	tok := ""
	for isLetter(rune(ch)) || isDigit(rune(ch)) {
		tok += string(ch)
		pos++
		if l.eof(pos) {
			break
		}
		ch = l.input[pos]
	}
	l.pos += len(tok)
	upperTok := strings.ToUpper(tok)
	token, ok := tokens[upperTok]
	if ok {
		l.lastToken = []byte(tok)
		return token, tok
	}
	return IDENTIFIER, tok
}

func Parse(sql string) (Statement, error) {
	lexer := NewLexer(strings.TrimSpace(sql))
	parse := yyParse(lexer)
	if parse != 0 {
		return nil, lexer.lastError
	}
	return lexer.ParseTree, nil
}
