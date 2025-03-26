package halfvec

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/x448/float16"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type HalfVector struct {
	data   []byte
	length int
}

// NewFromFloat32 creates a new HalfVector from a slice of float32 values
func NewFromFloat32(values []float32) *HalfVector {
	buf := make([]byte, 4+(2*len(values)))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(values)))
	for i, v := range values {
		fv := float16.Fromfloat32(v)
		binary.BigEndian.PutUint16(buf[4+(i*2):6+(i*2)], uint16(fv))
	}
	return &HalfVector{data: buf, length: len(values)}
}

func bytes16ToFloat32(b []byte) float32 {
	bits := binary.BigEndian.Uint16(b)
	return float16.Frombits(bits).Float32()
}

// ToFloat32 converts the HalfVector back to float32 values
func (hv *HalfVector) ToFloat32() []float32 {
	floatValues := make([]float32, hv.length)
	for i := 0; i < hv.length; i++ {
		start := 4 + (i * 2)
		floatValues[i] = bytes16ToFloat32(hv.data[start : start+2])
	}
	return floatValues
}

// EncodeBinary encodes the HalfVector to a binary format
// Format: [4 bytes length][2 bytes per value]
func (hv *HalfVector) EncodeBinary() ([]byte, error) {
	return hv.data, nil
}

func (hv *HalfVector) GormDataType() string {
	return "BINARY(1028)"
}

func (HalfVector) GormDBDataType(db *gorm.DB, field *schema.Field) string {
	switch db.Dialector.Name() {
	case "postgres":
		return "halfvec"
	default:
		return ""
	}
}

func (hv *HalfVector) Scan(value interface{}) error {
	valueBytes, ok := value.([]byte)
	if !ok {
		return errors.New("unable to convert value to bytes")
	}

	ohv, err := DecodeBinary(valueBytes)
	if err != nil {
		return err
	}

	hv.data = ohv.data
	hv.length = ohv.length
	return nil
}

// DecodeBinary decodes a binary representation back to a HalfVector
func DecodeBinary(data []byte) (*HalfVector, error) {
	dimBytes := data[:2]
	body := data[4:]

	length := binary.BigEndian.Uint16(dimBytes)

	if length < 0 {
		return nil, fmt.Errorf("invalid vector length: %d", length)
	}

	if len(body) != 2*int(length) {
		return nil, fmt.Errorf("body must be 2 bytes per item")
	}

	return &HalfVector{data: data, length: int(length)}, nil
}

type VectorCodec struct{}

func (c *VectorCodec) FormatSupported(format int16) bool {
	return format == pgtype.BinaryFormatCode
}

func (c *VectorCodec) PreferredFormat() int16 {
	return pgtype.BinaryFormatCode
}

// Implements pgtype.Codec
func (c *VectorCodec) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (interface{}, error) {
	if src == nil {
		return nil, nil
	}

	if format != pgtype.BinaryFormatCode {
		return nil, fmt.Errorf("unsupported format code: %d", format)
	}

	// Decode the binary data into our HalfVector type
	halfVec, err := DecodeBinary(src)
	if err != nil {
		return nil, err
	}

	return halfVec, nil
}

// VectorEncodePlan implements pgtype.EncodePlan
type VectorEncodePlan struct {
	target any
}

func (p *VectorEncodePlan) Encode(value any, buf []byte) ([]byte, error) {
	var v any
	if value != nil {
		v = value
	} else {
		v = p.target
	}

	switch val := v.(type) {
	case *HalfVector:
		encoded, err := val.EncodeBinary()
		if err != nil {
			return nil, err
		}
		return append(buf, encoded...), nil
	case HalfVector:
		encoded, err := (&val).EncodeBinary()
		if err != nil {
			return nil, err
		}
		return append(buf, encoded...), nil
	default:
		return nil, fmt.Errorf("unsupported source type %T", v)
	}
}

func (c *VectorCodec) PlanEncode(m *pgtype.Map, oid uint32, format int16, value any) pgtype.EncodePlan {
	if format != pgtype.BinaryFormatCode {
		return nil
	}

	// Simply return a VectorEncodePlan which will use our EncodeBinary method
	return &VectorEncodePlan{target: value}
}

// VectorScanPlan implements pgtype.ScanPlan
type VectorScanPlan struct {
	Target any
}

func (p *VectorScanPlan) Scan(src []byte, dst any) error {
	if src == nil {
		return nil
	}

	maxBytes := 10
	if len(src) < maxBytes {
		maxBytes = len(src)
	}
	fmt.Printf("DEBUG: Scanning with VectorScanPlan, src length: %d, first few bytes: %v\n", len(src), src[:maxBytes])

	// Decode the binary data into our HalfVector type
	halfVec, err := DecodeBinary(src)
	if err != nil {
		return fmt.Errorf("failed to decode binary data: %w", err)
	}

	switch v := dst.(type) {
	case **HalfVector:
		*v = halfVec
		return nil
	case *HalfVector:
		*v = *halfVec
		return nil
	default:
		return fmt.Errorf("unsupported target type %T", dst)
	}
}

func (c *VectorCodec) PlanScan(m *pgtype.Map, oid uint32, format int16, target any) pgtype.ScanPlan {
	fmt.Printf("DEBUG: PlanScan called with format: %d, target type: %T\n", format, target)

	if format != pgtype.BinaryFormatCode && format != pgtype.TextFormatCode {
		return nil
	}

	// Return a plan based on the target type
	switch target.(type) {
	case *[]float32, **HalfVector, *HalfVector:
		return &VectorScanPlan{Target: target}
	default:
		return nil
	}
}

func (c *VectorCodec) DecodeDatabaseSQLValue(m *pgtype.Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}

	if format != pgtype.BinaryFormatCode {
		return nil, fmt.Errorf("unsupported format code: %d", format)
	}

	// Decode the binary data into our HalfVector type
	halfVec, err := DecodeBinary(src)
	if err != nil {
		return nil, err
	}

	return halfVec.ToFloat32(), nil
}

func (c *VectorCodec) EncodeDatabaseSQLValue(m *pgtype.Map, oid uint32, format int16, src driver.Value) ([]byte, error) {
	if src == nil {
		return nil, nil
	}

	if format != pgtype.BinaryFormatCode {
		return nil, fmt.Errorf("unsupported format code: %d", format)
	}

	switch v := src.(type) {
	case *HalfVector:
		return v.EncodeBinary()
	case HalfVector:
		return (&v).EncodeBinary()
	default:
		return nil, fmt.Errorf("unsupported source type %T", src)
	}
}
