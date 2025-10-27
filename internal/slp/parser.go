package slp

import "fmt"

type ATOM string

const (
	ATOM_LIST_START = "("
	ATOM_LIST_END   = ")"
	ATOM_NONE       = "_"
	ATOM_SOME       = "*"
)

type Parser struct {
	Target   string
	Position int
}

func (p *Parser) Parse() Obj {
	p.skipWhitespace()

	if p.Position >= len(p.Target) {
		return Obj{Type: OBJ_TYPE_NONE, D: None{}}
	}

	switch p.Target[p.Position] {
	case '(':
		return p.parseList()
	case '\'':
		p.Position++        // consume the quote
		quoted := p.Parse() // parse the expression after the quote
		return Obj{Type: OBJ_TYPE_SOME, D: Some(quoted)}
	case '_':
		p.Position++
		return Obj{Type: OBJ_TYPE_NONE, D: None{}}
	default:
		return p.parseSome()
	}
}

func (p *Parser) parseList() Obj {
	listStart := p.Position
	p.Position++ // skip '('
	var items List

	for p.Position < len(p.Target) {
		p.skipWhitespace()
		if p.Position >= len(p.Target) {
			return Obj{Type: OBJ_TYPE_ERROR, D: Error{
				Position: listStart,
				Message:  "unclosed list",
			}}
		}
		if p.Target[p.Position] == ')' {
			p.Position++ // skip ')'
			return Obj{Type: OBJ_TYPE_LIST, D: items}
		}
		item := p.Parse()
		if item.Type == OBJ_TYPE_ERROR {
			return item
		}
		items = append(items, item)
	}

	return Obj{Type: OBJ_TYPE_ERROR, D: Error{
		Position: listStart,
		Message:  "unclosed list",
	}}
}

func (p *Parser) parseSome() Obj {
	if p.Target[p.Position] == '"' {
		return p.parseQuotedString()
	}

	start := p.Position
	for p.Position < len(p.Target) &&
		p.Target[p.Position] != ' ' &&
		p.Target[p.Position] != ')' &&
		p.Target[p.Position] != '(' {
		p.Position++
	}

	value := p.Target[start:p.Position]

	// Check if it's a number
	if num, ok := parseNumber(value); ok {
		return Obj{Type: OBJ_TYPE_NUMBER, D: num}
	}

	// Otherwise it's an identifier
	return Obj{Type: OBJ_TYPE_IDENTIFIER, D: Identifier(value)}
}

func (p *Parser) parseQuotedString() Obj {
	p.Position++ // skip opening quote
	start := p.Position

	for p.Position < len(p.Target) {
		if p.Target[p.Position] == '"' {
			// Check if it's escaped
			escapeCount := 0
			for i := p.Position - 1; i >= start && p.Target[i] == '\\'; i-- {
				escapeCount++
			}
			if escapeCount%2 == 0 {
				value := p.Target[start:p.Position]
				p.Position++ // skip closing quote
				unescaped := unescapeString(value)
				return Obj{Type: OBJ_TYPE_STRING, D: unescaped}
			}
		}
		p.Position++
	}

	return Obj{Type: OBJ_TYPE_ERROR, D: Error{
		Position: start - 1,
		Message:  "unclosed quoted string",
	}}
}

func unescapeString(s string) string {
	result := ""
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 'n':
				result += "\n"
			case 't':
				result += "\t"
			case 'r':
				result += "\r"
			case '"':
				result += "\""
			case '\\':
				result += "\\"
			default:
				result += string(s[i+1])
			}
			i++
		} else {
			result += string(s[i])
		}
	}
	return result
}

func parseNumber(s string) (Number, bool) {
	if s == "" {
		return 0, false
	}

	num, err := parseFloat(s)
	if err != nil {
		return 0, false
	}

	return Number(num), true
}

func parseFloat(s string) (float64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty string")
	}

	negative := false
	if s[0] == '-' {
		negative = true
		s = s[1:]
	} else if s[0] == '+' {
		s = s[1:]
	}

	if s == "" {
		return 0, fmt.Errorf("just sign")
	}

	var result float64
	var i int

	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		result = result*10 + float64(s[i]-'0')
		i++
	}

	if i < len(s) && s[i] == '.' {
		i++
		divisor := 1.0
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			divisor *= 10
			result += float64(s[i]-'0') / divisor
			i++
		}
	}

	if i != len(s) {
		return 0, fmt.Errorf("invalid character at position %d", i)
	}

	if negative {
		result = -result
	}

	return result, nil
}

func (p *Parser) skipWhitespace() {
	for p.Position < len(p.Target) && p.Target[p.Position] == ' ' {
		p.Position++
	}
}
