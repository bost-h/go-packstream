package packstream

import (
	"io"
	"math"
	"testing"
)

type testValue struct {
	Encoded []byte
	Decoded interface{}
}

type marshaller int

func (v *marshaller) MarshalPS() ([]byte, error) {
	return []byte{42}, nil
}

func (v *marshaller) UnmarshalPS(byte, io.Reader) error {
	*v = marshaller(42)
	return nil
}

var validTestValues = []testValue{
	// Null values
	{[]byte{mNull}, nil},

	// Boolean values
	{[]byte{mTrue}, true},
	{[]byte{mFalse}, false},

	// Float values
	{[]byte{mFloat64, 0x3F, 0xF1, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9A}, 1.1},
	{[]byte{mFloat64, 0xBF, 0xF1, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9A}, -1.1},
	{[]byte{mFloat64, 0xBF, 0xF1, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9A}, float64(-1.1)},

	// Int values
	{[]byte{mInt8, 0x80}, int64(math.MinInt8)},
	// TinyInt
	{[]byte{0xF0}, int64(minTinyInt)},
	{[]byte{0x7F}, int64(math.MaxInt8)},
	// Int8
	{[]byte{mInt8, 0x80}, int64(math.MinInt8)},
	{[]byte{mInt8, 0xEF}, int64(minTinyInt - 1)},
	// Int16
	{[]byte{mInt16, 0X80, 0X00}, int64(math.MinInt16)},
	{[]byte{mInt16, 0XFF, 0X7F}, int64(math.MinInt8 - 1)},
	{[]byte{mInt16, 0X00, 0X80}, int64(math.MaxInt8 + 1)},
	{[]byte{mInt16, 0X7F, 0XFF}, int64(math.MaxInt16)},
	// Int32
	{[]byte{mInt32, 0X80, 0X00, 0X00, 0X00}, int64(math.MinInt32)},
	{[]byte{mInt32, 0XFF, 0XFF, 0X7F, 0XFF}, int64(math.MinInt16 - 1)},
	{[]byte{mInt32, 0X00, 0X00, 0X80, 0X00}, int64(math.MaxInt16 + 1)},
	{[]byte{mInt32, 0X7F, 0XFF, 0XFF, 0XFF}, int64(math.MaxInt32)},
	// Int64
	{[]byte{mInt64, 0X80, 0X00, 0X00, 0X00, 0X00, 0X00, 0X00, 0X00}, int64(math.MinInt64)},
	{[]byte{mInt64, 0XFF, 0XFF, 0XFF, 0XFF, 0X7F, 0XFF, 0XFF, 0XFF}, int64(math.MinInt32 - 1)},
	{[]byte{mInt64, 0X00, 0X00, 0X00, 0X00, 0X80, 0X00, 0X00, 0X00}, int64(math.MaxInt32 + 1)},
	{[]byte{mInt64, 0X7F, 0XFF, 0XFF, 0XFF, 0XFF, 0XFF, 0XFF, 0XFF}, int64(math.MaxInt64)},

	// String values
	{[]byte{0X8F, 0X31, 0X32, 0X33, 0X34, 0X35, 0X36, 0X37, 0X38, 0X39, 0X61, 0X62, 0X63, 0X64, 0X65, 0X66}, "123456789abcdef"},
	{[]byte{0x80}, ""},
	{[]byte{0x81, 0x61}, "a"},
	{[]byte{mStringSize8, 0x1A, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7A},
		"abcdefghijklmnopqrstuvwxyz"},
	{[]byte{mStringSize8, 0x18, 0x45, 0x6E, 0x20, 0xC3, 0xA5, 0x20, 0x66, 0x6C, 0xC3, 0xB6, 0x74, 0x20, 0xC3, 0xB6, 0x76, 0x65, 0x72, 0x20, 0xC3, 0xA4, 0x6E, 0x67, 0x65, 0x6E},
		"En å flöt över ängen"},

	// List values
	{[]byte{0x91, 0x2A}, []interface{}{int64(42)}},
	{[]byte{0x9F, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0}, make([]interface{}, maxInt4-1)},
	{[]byte{0X91, 0X91, 0X85, 0X68, 0X65, 0X6C, 0X6C, 0X6F}, []interface{}{[]interface{}{"hello"}}},

	//Map values
	{[]byte{0xA1, 0x82, 0x34, 0x32, 0x2A}, map[string]interface{}{"42": int64(42)}},
	{[]byte{0xA1, 0x82, 0x34, 0x32, 0xA1, 0x82, 0x34, 0x32, 0x2A}, map[string]interface{}{"42": map[string]interface{}{"42": int64(42)}}},

	// Structure values
	{[]byte{0xB2, 0x2A, 0x85, 0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x91, 0x37}, Structure{Signature: 42, Fields: []interface{}{"hello", []interface{}{int64(55)}}}},
	{[]byte{0xBF, 0x2A, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0}, Structure{Signature: 42, Fields: make([]interface{}, maxInt4-1)}},
	{[]byte{0xDC, 0x10, 0x2A, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0}, Structure{Signature: 42, Fields: make([]interface{}, maxInt4)}},

	// Byte slice values
	{[]byte{mBytesSize8, 0x03, 0x01, 0x02, 0x03}, []byte{1, 2, 3}},
}

func TestNewStructure(t *testing.T) {
	if st := NewStructure(42, 1, 2); st == nil {
		t.Error("returned value should not be nil.")
	} else if st.Signature != 42 {
		t.Errorf("signature should equals %v, got %v", 42, st.Signature)
	} else if len(st.Fields) != 2 {
		t.Errorf("fields len should equals %v, got %v", 3, len(st.Fields))
	} else if st.Fields[0] != 1 {
		t.Errorf("fields[0] should equals %v, got %v", 1, st.Fields[0])
	} else if st.Fields[1] != 2 {
		t.Errorf("fields[1] should equals %v, got %v", 2, st.Fields[1])
	}
}
