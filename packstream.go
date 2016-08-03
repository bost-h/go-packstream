/*
Package packstream contains a full implementation of PackStream: the serialisation
format built specifically for Neo4j. The PackStream design is based heavily on
MessagePack but the implementation completely separate.

Install with:

	go get gopkg.in/packstream.v1

Then import with:

	import "gopkg.in/packstream.v1"
*/
package packstream

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"reflect"
)

const (
	mTinyStringStart = 0x80
	mTinyStringEnd   = 0x8F
	mStringSize8     = 0xD0
	mStringSize16    = 0xD1
	mStringSize32    = 0xD2

	mTinyListStart  = 0x90
	mTinyListEnd    = 0x9F
	mListSize8      = 0xD4
	mListSize16     = 0xD5
	mListSize32     = 0xD6
	mListSizeStream = 0xD7

	mTinyMapStart  = 0xA0
	mTinyMapEnd    = 0xAF
	mMapSize8      = 0xD8
	mMapSize16     = 0xD9
	mMapSize32     = 0xDA
	mMapSizeStream = 0xDB

	mTinyStructStart = 0xB0
	mTinyStructEnd   = 0xBF
	mStructSize8     = 0xDC
	mStructSize16    = 0xDD

	mBytesSize8  = 0xCC
	mBytesSize16 = 0xCD
	mBytesSize32 = 0xCE

	mNull    = 0xC0
	mFloat64 = 0xC1
	mFalse   = 0xC2
	mTrue    = 0xC3
	mInt8    = 0xC8
	mInt16   = 0xC9
	mInt32   = 0xCA
	mInt64   = 0xCB

	maxInt4    = 16
	minTinyInt = -16

	mEndOfStream = 0xDF
)

// ErrMarshalTypeError is returned when encoding an unsupported type.
var ErrMarshalTypeError = errors.New("marshal: unsupported type")

// ErrUnMarshalTypeError is returned when decoding a packstream value into a not appropriate type
var ErrUnMarshalTypeError = errors.New("marshal: inappropriate type")

// ErrMarshalValueTooLarge is returned when encoding a value which is too large for packstream format.
var ErrMarshalValueTooLarge = errors.New("marshal: value is too large for packstream encoding")

var (
	// Packed sizes
	tinyStringSizes   [][]byte
	tinyStructSizes   [][]byte
	tinyMapSizes      [][]byte
	tinyListSizes     [][]byte
	packedUint8Sizes  [][]byte
	packedUint16Sizes [][]byte
	packedUint32Size  func(n uint32) []byte
	structType        reflect.Type
)

// Marshaler is the interface implemented by objects that can marshal themselves into packstream.
type Marshaler interface {
	MarshalPS() ([]byte, error)
}

// Unmarshaler is the interface implemented by objects that can unmarshal a packstream description of themselves.
//
// UnmarshalPS receives the byte packstream marker, and a io.Reader. It must only read the bytes they need.
type Unmarshaler interface {
	UnmarshalPS(byte, io.Reader) error
}

// Structure represents a packstream structure.
type Structure struct {
	Signature byte          // Signature is the structure signature.
	Fields    []interface{} // Fields are the structure fields.
}

func init() {
	structType = reflect.TypeOf(Structure{})
	tinyStringSizes = make([][]byte, mTinyStringEnd)
	for i := mTinyStringStart; i <= mTinyStringEnd; i++ {
		tinyStringSizes[i-mTinyStringStart] = []byte{byte(i)}
	}
	tinyStructSizes = make([][]byte, mTinyStructEnd)
	for i := mTinyStructStart; i <= mTinyStructEnd; i++ {
		tinyStructSizes[i-mTinyStructStart] = []byte{byte(i)}
	}
	tinyMapSizes = make([][]byte, mTinyMapEnd)
	for i := mTinyMapStart; i <= mTinyMapEnd; i++ {
		tinyMapSizes[i-mTinyMapStart] = []byte{byte(i)}
	}
	tinyListSizes = make([][]byte, mTinyListEnd)
	for i := mTinyListStart; i <= mTinyListEnd; i++ {
		tinyListSizes[i-mTinyListStart] = []byte{byte(i)}
	}
	packedUint8Sizes = make([][]byte, math.MaxUint8+1)
	for i := 0; i <= math.MaxUint8; i++ {
		packedUint8Sizes[i] = []byte{byte(i)}
	}
	packedUint16Sizes = make([][]byte, math.MaxUint16+1)
	for i := math.MaxUint8 + 1; i <= math.MaxUint16; i++ {
		packedUint16Sizes[i] = make([]byte, 2)
		binary.BigEndian.PutUint16(packedUint16Sizes[i], uint16(i))
	}
	packedUint32Size = func(n uint32) []byte {
		p := make([]byte, 4)
		binary.BigEndian.PutUint32(p, n)
		return p
	}
}

// NewStructure returns a new structure with the given signature and optional fields.
func NewStructure(signature byte, fields ...interface{}) *Structure {
	return &Structure{
		Signature: signature,
		Fields:    fields,
	}
}
