package main

import (
	"strings"
)

func separateInput(input string) []inputStatement {
	return newSeparator(input).separate()
}

type separator struct {
	str []rune // remaining input
	sb  *strings.Builder
}

func newSeparator(s string) *separator {
	return &separator{
		str: []rune(s),
		sb:  &strings.Builder{},
	}
}

func (s *separator) consumeRawString() {
	// consume 'r' or 'R'
	s.sb.WriteRune(s.str[0])
	s.str = s.str[1:]

	delim := s.consumeStringDelimiter()
	s.consumeStringContent(delim, true)
}

func (s *separator) consumeBytesString() {
	// consume 'b' or 'B'
	s.sb.WriteRune(s.str[0])
	s.str = s.str[1:]

	delim := s.consumeStringDelimiter()
	s.consumeStringContent(delim, false)
}

func (s *separator) consumeRawBytesString() {
	// consume 'rb', 'Rb', 'rB', or 'RB'
	s.sb.WriteRune(s.str[0])
	s.sb.WriteRune(s.str[1])
	s.str = s.str[2:]

	delim := s.consumeStringDelimiter()
	s.consumeStringContent(delim, true)
}

func (s *separator) consumeString() {
	delim := s.consumeStringDelimiter()
	s.consumeStringContent(delim, false)
}

func (s *separator) consumeStringContent(delim string, raw bool) {
	var i int
	for i < len(s.str) {
		// check end of string
		switch {
		// delimiter is `"` or `'`
		case len(delim) == 1 && string(s.str[i]) == delim:
			s.str = s.str[i+1:]
			s.sb.WriteString(delim)
			return
		// delimiter is `"""` or `'''`
		case len(delim) == 3 && len(s.str) >= i+3 && string(s.str[i:i+3]) == delim:
			s.str = s.str[i+3:]
			s.sb.WriteString(delim)
			return
		}

		// escape sequence
		if s.str[i] == '\\' {
			if raw {
				// raw string treats escape character as backslash
				s.sb.WriteRune('\\')
				i++
				continue
			}

			// invalid escape sequence
			if i+1 >= len(s.str) {
				s.sb.WriteRune('\\')
				return
			}

			s.sb.WriteRune('\\')
			s.sb.WriteRune(s.str[i+1])
			i+=2
			continue
		}
		s.sb.WriteRune(s.str[i])
		i++
	}
	s.str = s.str[i:]
	return
}

func (s *separator) consumeStringDelimiter() string {
	c := s.str[0]
	// check triple-quoted delimiter
	if len(s.str) >= 3 && s.str[1] == c && s.str[2] == c {
		delim := strings.Repeat(string(c), 3)
		s.sb.WriteString(delim)
		s.str = s.str[3:]
		return delim
	}
	s.str = s.str[1:]
	s.sb.WriteRune(c)
	return string(c)
}

// separate separates input string into multiple Spanner statements.
// This does not validate syntax of statements.
//
// NOTE: Logic for parsing a statement is mostly taken from spansql.
// https://github.com/googleapis/google-cloud-go/blob/master/spanner/spansql/parser.go
func (s *separator) separate() []inputStatement {
	var statements []inputStatement
	for len(s.str) > 0 {
		switch s.str[0] {
		// possibly string literal
		case '"', '\'', 'r', 'R', 'b', 'B':
			// valid string prefix: "b", "B", "r", "R", "br", "bR", "Br", "BR"
			// https://cloud.google.com/spanner/docs/lexical#string_and_bytes_literals
			raw, bytes, str := false, false, false
			for i := 0; i < 3 && i < len(s.str); i++ {
				switch {
				case !raw && (s.str[i] == 'r' || s.str[i] == 'R'):
					raw = true
					continue
				case !bytes && (s.str[i] == 'b' || s.str[i] == 'B'):
					bytes = true
					continue
				case s.str[i] == '"' || s.str[i] == '\'':
					str = true
					switch {
					case raw && bytes:
						s.consumeRawBytesString()
					case raw:
						s.consumeRawString()
					case bytes:
						s.consumeBytesString()
					default:
						s.consumeString()
					}
				}
			}
			if !str {
				s.sb.WriteRune(s.str[0])
				s.str = s.str[1:]
			}
		// horizontal delimiter
		case ';':
			statements = append(statements, inputStatement{
				statement: strings.TrimSpace(s.sb.String()),
				delimiter: delimiterHorizontal,
			})
			s.sb.Reset()
			s.str = s.str[1:]
		// possibly vertical delimiter
		case '\\':
			if len(s.str) >= 2 && s.str[1] == 'G' {
				statements = append(statements, inputStatement{
					statement: strings.TrimSpace(s.sb.String()),
					delimiter: delimiterVertical,
				})
				s.sb.Reset()
				s.str = s.str[2:]
				continue
			}
			s.sb.WriteRune(s.str[0])
			s.str = s.str[1:]
		default:
			s.sb.WriteRune(s.str[0])
			s.str = s.str[1:]
		}
	}

	// flush remained
	if s.sb.Len() > 0 {
		if str := strings.TrimSpace(s.sb.String()); len(str) > 0 {
			// use horizontal delimiter for the statement without explicit delimiter
			statements = append(statements, inputStatement{
				statement: str,
				delimiter: delimiterHorizontal,
			})
			s.sb.Reset()
		}
	}
	return statements
}
