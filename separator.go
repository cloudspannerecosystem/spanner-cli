package main

import (
	"log"
	"strings"
)

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
	s.consumeStringContent(delim, false)
}

func (s *separator) consumeString() {
	delim := s.consumeStringDelimiter()
	s.consumeStringContent(delim, false)
}

func (s *separator) consumeStringContent(delim string, raw bool) error {
	var i int
	for i < len(s.str) {
		if strings.HasPrefix(string(s.str[i:]), delim) {
			log.Printf("close: remained %s", string(s.str))
			i += len(delim)
			s.str = s.str[i:]
			log.Printf("after close: remained %s", string(s.str))
			s.sb.WriteString(delim)
			return nil
		}

		// escape character
		if s.str[i] == '\\' {
			i++
			if i >= len(s.str) {
				// TODO
			}

			if raw {
				// raw string treats escape character as backslash
				s.sb.WriteRune('\\')
				i++
				continue
			}

			s.sb.WriteRune('"')
			i++
			continue
		}
		s.sb.WriteRune(s.str[i])
		i++
	}
	return nil
}

func (s *separator) consumeStringDelimiter() string {
	c := s.str[0]
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
//
// Logic for parsing a statement is mostly taken from spansql.
// https://github.com/googleapis/google-cloud-go/blob/master/spanner/spansql/parser.go
func (s *separator) separate() ([]inputStatement, error) {
	var statements []inputStatement
	for len(s.str) > 0 {
		switch s.str[0] {
		// string literal
		case '"', '\'', 'r', 'R', 'b', 'B':
			// valid prefix: "b", "B", "r", "R", "br", "bR", "Br", "BR"
			raw := false
			for i := 0; i < 3 && i < len(s.str); i++ {
				switch {
				case !raw && (s.str[i] == 'r' || s.str[i] == 'R'):
					raw = true
					continue
				case s.str[i] == '"' || s.str[i] == '\'':
					if raw {
						s.consumeRawString()
					} else {
						s.consumeString()
						log.Printf("consumed: remained %s", string(s.str))
					}
				default:
					s.sb.WriteRune(s.str[0])
					s.str = s.str[1:]
				}
				break
			}
		case ';':
			log.Printf("delimiter found: remained %s", string(s.str))
			statements = append(statements, inputStatement{
				statement: strings.TrimSpace(s.sb.String()),
				delimiter: delimiterHorizontal,
			})
			s.sb.Reset()
			s.str = s.str[1:]
		case '\\':
			if strings.HasPrefix(string(s.str), `\G`) {
				statements = append(statements, inputStatement{
					statement: strings.TrimSpace(s.sb.String()),
					delimiter: delimiterVertical,
				})
				s.sb.Reset()
				s.str = s.str[2:]
			} else {
				// TODO
			}
		default:
			s.sb.WriteRune(s.str[0])
			s.str = s.str[1:]
		}
	}

	if s.sb.Len() > 0 {
		// flush remained
		statements = append(statements, inputStatement{
			statement: strings.TrimSpace(s.sb.String()),
			delimiter: delimiterHorizontal,
		})
		s.sb.Reset()
	}
	return statements, nil
}
