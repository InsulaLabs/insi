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
	target   string
	position int
}

func (p *Parser) Parse() Obj {
	p.skipWhitespace()

	if p.position >= len(p.target) {
		return Obj{Type: OBJ_TYPE_NONE, D: None{}}
	}

	switch p.target[p.position] {
	case '(':
		return p.parseList()
	case '\'':
		p.position++        // consume the quote
		quoted := p.Parse() // parse the expression after the quote
		return Obj{Type: OBJ_TYPE_SOME, D: Some(quoted)}
	case '_':
		p.position++
		return Obj{Type: OBJ_TYPE_NONE, D: None{}}
	default:
		return p.parseSome()
	}
}

func (p *Parser) parseList() Obj {
	listStart := p.position
	p.position++ // skip '('
	var items List

	for p.position < len(p.target) {
		p.skipWhitespace()
		if p.position >= len(p.target) {
			return Obj{Type: OBJ_TYPE_ERROR, D: Error{
				Position: listStart,
				Message:  "unclosed list",
			}}
		}
		if p.target[p.position] == ')' {
			p.position++ // skip ')'
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
	if p.target[p.position] == '"' {
		return p.parseQuotedString()
	}

	start := p.position
	for p.position < len(p.target) &&
		p.target[p.position] != ' ' &&
		p.target[p.position] != ')' &&
		p.target[p.position] != '(' {
		p.position++
	}

	value := p.target[start:p.position]

	// Check if it's a number
	if num, ok := parseNumber(value); ok {
		return Obj{Type: OBJ_TYPE_NUMBER, D: num}
	}

	// Otherwise it's an identifier
	return Obj{Type: OBJ_TYPE_IDENTIFIER, D: Identifier(value)}
}

func (p *Parser) parseQuotedString() Obj {
	p.position++ // skip opening quote
	start := p.position

	for p.position < len(p.target) {
		if p.target[p.position] == '"' {
			// Check if it's escaped
			escapeCount := 0
			for i := p.position - 1; i >= start && p.target[i] == '\\'; i-- {
				escapeCount++
			}
			if escapeCount%2 == 0 {
				value := p.target[start:p.position]
				p.position++ // skip closing quote
				unescaped := unescapeString(value)
				return Obj{Type: OBJ_TYPE_STRING, D: unescaped}
			}
		}
		p.position++
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
	for p.position < len(p.target) && p.target[p.position] == ' ' {
		p.position++
	}
}
