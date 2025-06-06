package slp

import (
	"fmt"
	"strconv"
	"strings"
)

type DataType string

const (
	DataTypeIdentifier DataType = "identifier"
	DataTypeString     DataType = "string"
	DataTypeNumber     DataType = "number"
	DataTypeList       DataType = "list"
	DataTypeNull       DataType = "null"
	DataTypeFunction   DataType = "function"
)

type Value struct {
	Type  DataType
	Value interface{}
}

type Node struct {
	Next *Node
	Prev *Node
	Data Value
}

type List struct {
	Head *Node
	Tail *Node
}

type Program struct {
	Lists []*List
}

// ParseError represents an error during parsing, with position.
type ParseError struct {
	Position int
	Message  string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error at position %d: %s", e.Position, e.Message)
}

// parser holds the state of the parsing process.
type parser struct {
	input string
	pos   int
}

func (p *parser) isAtEnd() bool {
	return p.pos >= len(p.input)
}

func (p *parser) peek() byte {
	if p.isAtEnd() {
		return 0
	}
	return p.input[p.pos]
}

func (p *parser) peekNext() byte {
	if p.pos+1 >= len(p.input) {
		return 0
	}
	return p.input[p.pos+1]
}

func (p *parser) advance() {
	if !p.isAtEnd() {
		p.pos++
	}
}

func (p *parser) skipWhitespace() {
	for !p.isAtEnd() {
		switch p.peek() {
		case ' ', '\t', '\n', '\r':
			p.advance()
		default:
			return
		}
	}
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func isIdentifierStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isIdentifierChar(c byte) bool {
	return isIdentifierStart(c) || isDigit(c) || c == '-'
}

func (p *parser) parseString() (string, error) {
	p.advance() // consume opening '"'
	var builder strings.Builder
	start := p.pos
	for p.peek() != '"' && !p.isAtEnd() {
		if p.peek() == '\\' {
			builder.WriteString(p.input[start:p.pos])
			p.advance() // consume '\'
			if p.isAtEnd() {
				return "", &ParseError{Position: p.pos, Message: "unterminated string escape"}
			}
			switch p.peek() {
			case 'n':
				builder.WriteByte('\n')
			case 't':
				builder.WriteByte('\t')
			case '"':
				builder.WriteByte('"')
			case '\\':
				builder.WriteByte('\\')
			default:
				return "", &ParseError{Position: p.pos, Message: fmt.Sprintf("unsupported escape sequence: \\%c", p.peek())}
			}
			p.advance()
			start = p.pos
		} else {
			p.advance()
		}
	}

	if p.isAtEnd() {
		return "", &ParseError{Position: p.pos, Message: "unterminated string"}
	}

	builder.WriteString(p.input[start:p.pos])
	p.advance() // consume closing '"'
	return builder.String(), nil
}

func (p *parser) parseNumber() (interface{}, error) {
	start := p.pos
	if p.peek() == '-' {
		p.advance()
	}

	for isDigit(p.peek()) {
		p.advance()
	}

	isFloat := false
	if p.peek() == '.' {
		isFloat = true
		p.advance()
		if !isDigit(p.peek()) {
			return nil, &ParseError{Position: p.pos, Message: "invalid float number format, expected digit after '.'"}
		}
		for isDigit(p.peek()) {
			p.advance()
		}
	}

	numStr := p.input[start:p.pos]
	if len(numStr) == 1 && numStr[0] == '-' {
		return nil, &ParseError{Position: start, Message: "invalid number format, dangling '-'"}
	}

	if isFloat {
		val, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			return nil, &ParseError{Position: start, Message: "invalid float number format"}
		}
		return val, nil
	}

	val, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return nil, &ParseError{Position: start, Message: "invalid integer number format"}
	}
	return val, nil
}

func (p *parser) parseIdentifierOrNull() (interface{}, error) {
	start := p.pos
	for !p.isAtEnd() && isIdentifierChar(p.peek()) {
		p.advance()
	}

	identifier := p.input[start:p.pos]
	if identifier == "null" {
		return nil, nil // Represents DataTypeNull
	}

	return identifier, nil
}

func (p *parser) parseElement() (Value, error) {
	p.skipWhitespace()
	pos := p.pos

	switch p.peek() {
	case '"':
		s, err := p.parseString()
		if err != nil {
			return Value{}, err
		}
		return Value{Type: DataTypeString, Value: s}, nil
	case '(':
		p.advance()
		l, err := p.parseListBody()
		if err != nil {
			return Value{}, err
		}
		return Value{Type: DataTypeList, Value: l}, nil
	default:
		if isDigit(p.peek()) || p.peek() == '-' {
			n, err := p.parseNumber()
			if err != nil {
				return Value{}, err
			}
			return Value{Type: DataTypeNumber, Value: n}, nil
		}
		if isIdentifierStart(p.peek()) {
			i, err := p.parseIdentifierOrNull()
			if err != nil {
				return Value{}, err
			}
			if i == nil {
				return Value{Type: DataTypeNull, Value: nil}, nil
			}
			return Value{Type: DataTypeIdentifier, Value: i}, nil
		}
		return Value{}, &ParseError{Position: pos, Message: fmt.Sprintf("unexpected character '%c'", p.peek())}
	}
}

func (p *parser) parseListBody() (*List, error) {
	list := &List{}
	p.skipWhitespace()

	for p.peek() != ')' {
		if p.isAtEnd() {
			return nil, &ParseError{Position: p.pos, Message: "unexpected end of input, expected ')'"}
		}

		element, err := p.parseElement()
		if err != nil {
			return nil, err
		}

		node := &Node{Data: element}
		if list.Head == nil {
			list.Head = node
			list.Tail = node
		} else {
			list.Tail.Next = node
			node.Prev = list.Tail
			list.Tail = node
		}

		p.skipWhitespace()
	}

	p.advance() // consume ')'
	return list, nil
}

func ParseBlock(block string) (*Program, string, error) {
	/*
		The block may or may not contain a program. Its a block of text from a user meant for an llm
		conversation. We need to detect if they have a "(\\'" symbol to indicate the start of a program.
		If so then we need to parse as a regular list until the final ")" symbol.
		We will identify numbers, strings, identifiers, null, and lists.
	*/
	program := &Program{}
	p := &parser{input: block, pos: 0}

	var commandSpans [][2]int
	offset := 0
	for {
		// We search from the current position in the block.
		remainingBlock := block[offset:]
		startIdx := strings.Index(remainingBlock, "(\\'")
		if startIdx == -1 {
			break
		}

		// Absolute start index of the command block in the original string
		blockStart := offset + startIdx

		// Set the parser position to the start of the list content.
		p.pos = blockStart + 3 // +3 for "(\\'"

		list, err := p.parseListBody()
		if err != nil {
			return nil, block, err
		}
		program.Lists = append(program.Lists, list)

		// After parsing, p.pos is at the end of the command block
		blockEnd := p.pos
		commandSpans = append(commandSpans, [2]int{blockStart, blockEnd})

		// Move offset for next search
		offset = p.pos
	}

	if len(program.Lists) == 0 {
		return nil, block, nil
	}

	// Build the scrubbed message by removing the command spans
	var builder strings.Builder
	lastIndex := 0
	for _, span := range commandSpans {
		builder.WriteString(block[lastIndex:span[0]])
		lastIndex = span[1]
	}
	builder.WriteString(block[lastIndex:])

	scrubbedMessage := strings.TrimSpace(builder.String())

	return program, scrubbedMessage, nil
}
