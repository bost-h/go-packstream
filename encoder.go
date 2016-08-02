package packstream

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"reflect"
)

// Encoder can write go values to an output stream, encoding them in packstream format.
type Encoder struct {
	wr io.Writer
}

// NewEncoder returns a new encoder that writes to wr.
func NewEncoder(wr io.Writer) *Encoder {
	return &Encoder{wr: wr}
}

/*
Marshal returns the packstream encoding of v.

Marshal traverses the value v recursively. If an encountered value is nil, then it encodes the nil value.
If an encountered value implements the Marshaler interface and is not a nil pointer, Marshal calls its MarshalPS method
to produce packstream bytes.

Marshal can encode the following go values:
	nil
	bool
	float32, float64
	int, int8, int16, int32, int64
	uint, uint8, uint16, uint32, uint64
	string
	[]byte
	[]interface{}
	map[string]interface{}
	Structure
*/
func Marshal(v interface{}) (p []byte, err error) {
	var b bytes.Buffer
	e := NewEncoder(&b)
	if err = e.Encode(v); err != nil {
		return
	}
	p = b.Bytes()
	return
}

// Encode writes a Go value to the underlying writer, encoding them in packstream format.
//
// See the documentation for Marshal for details about the conversion of Go values to packstream.
func (e *Encoder) Encode(v interface{}) (err error) {
	if v == nil {
		err = e.encodeNull()
	} else {
		err = e.encode(reflect.ValueOf(v))
	}
	return
}

func (e *Encoder) encode(rv reflect.Value) (err error) {
	for {
		if rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Slice || rv.Kind() == reflect.Map ||
			rv.Kind() == reflect.Interface {
			if rv.IsNil() {
				err = e.encodeNull()
				return
			}
		}
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		} else {
			break
		}
	}

	if rv.Kind() == reflect.Interface {
		if m, ok := rv.Interface().(Marshaler); ok {
			err = e.encodeMarshaler(m)
			return
		}
		return e.encode(rv.Elem())
	}

	switch rv.Kind() {
	default:
		err = ErrMarshalTypeError
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8,
		reflect.Uint16, reflect.Uint32, reflect.Uint64:
		err = e.encodeInt(rv)
	case reflect.Float32, reflect.Float64:
		err = e.encodeFloat(rv)
	case reflect.Bool:
		err = e.encodeBool(rv)
	case reflect.String:
		err = e.encodeString(rv)
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			err = e.encodeByteSlice(rv)
		} else {
			err = e.encodeList(rv)
		}
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			err = ErrMarshalTypeError
		} else {
			err = e.encodeMap(rv)
		}
	case reflect.Struct:
		if rv.Type() == structType {
			err = e.encodeStruct(rv)
		} else {
			err = ErrMarshalTypeError
		}
	}
	return
}

func (e *Encoder) encodeString(rv reflect.Value) (err error) {
	p := []byte(rv.String())
	n := len(p)
	switch {
	default:
		return ErrMarshalValueTooLarge
	case n < maxInt4:
		if _, err = e.wr.Write(tinyStringSizes[n]); err != nil {
			return
		}
	case n <= math.MaxUint8:
		if _, err = e.wr.Write([]byte{mStringSize8}); err != nil {
			return
		}
		if _, err = e.wr.Write(packedUint8Sizes[n]); err != nil {
			return
		}
	case n <= math.MaxUint16:
		if _, err = e.wr.Write([]byte{mStringSize16}); err != nil {
			return
		}
		if _, err = e.wr.Write(packedUint16Sizes[n]); err != nil {
			return
		}
	case n <= math.MaxUint32:
		if _, err = e.wr.Write([]byte{mStringSize32}); err != nil {
			return
		}
		if _, err = e.wr.Write(packedUint32Size(uint32(n))); err != nil {
			return
		}
	}
	_, err = e.wr.Write(p)
	return
}

func (e *Encoder) encodeByteSlice(rv reflect.Value) (err error) {
	p := rv.Bytes()
	n := len(p)
	switch {
	default:
		return ErrMarshalValueTooLarge
	case n <= math.MaxUint8:
		if _, err = e.wr.Write([]byte{mBytesSize8}); err != nil {
			return
		}
		if _, err = e.wr.Write(packedUint8Sizes[n]); err != nil {
			return
		}
	case n <= math.MaxUint16:
		if _, err = e.wr.Write([]byte{mBytesSize16}); err != nil {
			return
		}
		if _, err = e.wr.Write(packedUint16Sizes[n]); err != nil {
			return
		}
	case n <= math.MaxUint32:
		if _, err = e.wr.Write([]byte{mBytesSize32}); err != nil {
			return
		}
		if _, err = e.wr.Write(packedUint32Size(uint32(n))); err != nil {
			return
		}
	}
	_, err = e.wr.Write(p)
	return
}

func (e *Encoder) encodeStruct(rv reflect.Value) (err error) {
	sig := byte(rv.FieldByName("Signature").Uint())
	fields := rv.FieldByName("Fields")
	n := fields.Len()
	switch {
	default:
		return ErrMarshalValueTooLarge
	case n < maxInt4:
		if _, err = e.wr.Write(tinyStructSizes[n]); err != nil {
			return
		}
	case n <= math.MaxUint8:
		if _, err = e.wr.Write([]byte{mStructSize8}); err != nil {
			return
		}
		if _, err = e.wr.Write(packedUint8Sizes[n]); err != nil {
			return
		}
	case n <= math.MaxUint16:
		if _, err = e.wr.Write([]byte{mStructSize16}); err != nil {
			return
		}
		if _, err = e.wr.Write(packedUint16Sizes[n]); err != nil {
			return
		}
	}
	e.wr.Write([]byte{sig})

	for i := 0; i < n; i++ {
		if err = e.encode(fields.Index(i)); err != nil {
			return
		}
	}
	return
}

func (e *Encoder) encodeMap(rv reflect.Value) (err error) {
	n := rv.Len()
	switch {
	default:
		return ErrMarshalValueTooLarge
	case n < maxInt4:
		if _, err = e.wr.Write(tinyMapSizes[n]); err != nil {
			return
		}
	case n <= math.MaxUint8:
		if _, err = e.wr.Write([]byte{mMapSize8}); err != nil {
			return
		}
		if _, err = e.wr.Write(packedUint8Sizes[n]); err != nil {
			return
		}
	case n <= math.MaxUint16:
		if _, err = e.wr.Write([]byte{mMapSize16}); err != nil {
			return
		}
		if _, err = e.wr.Write(packedUint16Sizes[n]); err != nil {
			return
		}
	case n <= math.MaxUint32:
		if _, err = e.wr.Write([]byte{mMapSize32}); err != nil {
			return
		}
		if _, err = e.wr.Write(packedUint32Size(uint32(n))); err != nil {
			return
		}
	}
	for _, k := range rv.MapKeys() {
		if err = e.encode(k); err != nil {
			return
		}
		if err = e.encode(rv.MapIndex(k)); err != nil {
			return
		}
	}
	return
}

func (e *Encoder) encodeInt(rv reflect.Value) (err error) {
	var n int64
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n = rv.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		un := rv.Uint()
		if un > math.MaxInt64 {
			return ErrMarshalValueTooLarge
		}
		n = int64(un)
	}

	switch {
	case minTinyInt <= n && n <= math.MaxInt8:
		if _, err = e.wr.Write([]byte{byte(n)}); err != nil {
			return
		}
		return
	case math.MinInt8 <= n && n < minTinyInt:
		if _, err = e.wr.Write([]byte{mInt8}); err != nil {
			return
		}
		if err = binary.Write(e.wr, binary.BigEndian, int8(n)); err != nil {
			return
		}

	case math.MinInt16 <= n && n <= math.MaxInt16:
		if _, err = e.wr.Write([]byte{mInt16}); err != nil {
			return
		}
		if err = binary.Write(e.wr, binary.BigEndian, int16(n)); err != nil {
			return
		}
	case math.MinInt32 <= n && n <= math.MaxInt32:
		if _, err = e.wr.Write([]byte{mInt32}); err != nil {
			return
		}
		if err = binary.Write(e.wr, binary.BigEndian, int32(n)); err != nil {
			return
		}
	case math.MinInt64 <= n && n <= math.MaxInt64:
		if _, err = e.wr.Write([]byte{mInt64}); err != nil {
			return
		}
		if err = binary.Write(e.wr, binary.BigEndian, n); err != nil {
			return
		}
	}
	return
}

func (e *Encoder) encodeFloat(rv reflect.Value) (err error) {
	var p [8]byte
	n := rv.Float()
	if _, err = e.wr.Write([]byte{mFloat64}); err != nil {
		return
	}
	bits := math.Float64bits(n)
	binary.BigEndian.PutUint64(p[:], bits)
	_, err = e.wr.Write(p[:])
	return
}

func (e *Encoder) encodeBool(rv reflect.Value) (err error) {
	v := rv.Bool()
	if v {
		_, err = e.wr.Write([]byte{mTrue})
	} else {
		_, err = e.wr.Write([]byte{mFalse})
	}
	return
}

func (e *Encoder) encodeNull() (err error) {
	_, err = e.wr.Write([]byte{mNull})
	return
}

func (e *Encoder) encodeList(rv reflect.Value) (err error) {
	n := rv.Len()
	switch {
	default:
		return ErrMarshalValueTooLarge
	case n < maxInt4:
		if _, err = e.wr.Write(tinyListSizes[n]); err != nil {
			return
		}
	case n <= math.MaxUint8:
		if _, err = e.wr.Write([]byte{mListSize8}); err != nil {
			return
		}
		if _, err = e.wr.Write(packedUint8Sizes[n]); err != nil {
			return
		}
	case n <= math.MaxUint16:
		if _, err = e.wr.Write([]byte{mListSize16}); err != nil {
			return
		}
		if _, err = e.wr.Write(packedUint16Sizes[n]); err != nil {
			return
		}
	case n <= math.MaxUint32:
		if _, err = e.wr.Write([]byte{mListSize32}); err != nil {
			return
		}
		if _, err = e.wr.Write(packedUint32Size(uint32(n))); err != nil {
			return
		}
	}

	for i := 0; i < n; i++ {
		if err = e.encode(rv.Index(i)); err != nil {
			return
		}
	}
	return
}

func (e *Encoder) encodeMarshaler(v Marshaler) (err error) {
	var p []byte
	if p, err = v.MarshalPS(); err == nil {
		_, err = e.wr.Write(p)
		return nil
	}
	return
}
