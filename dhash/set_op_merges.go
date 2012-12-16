package dhash

import (
	"../common"
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
)

func (self *Node) getMerger(m common.SetOpMerge) mergeFunc {
	switch m {
	case common.Append:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			return append(oldValues, newValues...)
		}
	case common.ConCat:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			var res []byte
			for _, b := range oldValues {
				res = append(res, b...)
			}
			for _, b := range newValues {
				res = append(res, b...)
			}
			return [][]byte{res}
		}
	case common.IntegerSum:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			var sum int64
			var tmp int64
			var err error
			for _, b := range oldValues {
				if tmp, err = common.DecodeInt64(b); err == nil {
					sum += tmp
				}
			}
			for _, b := range newValues {
				if tmp, err = common.DecodeInt64(b); err == nil {
					sum += tmp
				}
			}
			res := new(bytes.Buffer)
			binary.Write(res, binary.BigEndian, sum)
			return [][]byte{res.Bytes()}
		}
	case common.IntegerSub:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			var sum int64
			var tmp int64
			var err error
			if tmp, err = common.DecodeInt64(oldValues[0]); err == nil {
				sum = tmp
			}
			for _, b := range oldValues[1:] {
				if tmp, err = common.DecodeInt64(b); err == nil {
					sum -= tmp
				}
			}
			for _, b := range newValues {
				if tmp, err = common.DecodeInt64(b); err == nil {
					sum -= tmp
				}
			}
			res := new(bytes.Buffer)
			binary.Write(res, binary.BigEndian, sum)
			return [][]byte{res.Bytes()}
		}
	case common.IntegerDiv:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			var sum int64
			var tmp int64
			var err error
			if tmp, err = common.DecodeInt64(oldValues[0]); err == nil {
				sum = tmp
			}
			for _, b := range oldValues[1:] {
				if tmp, err = common.DecodeInt64(b); err == nil {
					sum /= tmp
				}
			}
			for _, b := range newValues {
				if tmp, err = common.DecodeInt64(b); err == nil {
					sum /= tmp
				}
			}
			res := new(bytes.Buffer)
			binary.Write(res, binary.BigEndian, sum)
			return [][]byte{res.Bytes()}
		}
	case common.IntegerMul:
		fmt.Println("integer mul!")
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			var sum int64
			var tmp int64
			var err error
			if tmp, err = common.DecodeInt64(oldValues[0]); err == nil {
				sum = tmp
			}
			for _, b := range oldValues[1:] {
				if tmp, err = common.DecodeInt64(b); err == nil {
					sum *= tmp
				}
			}
			for _, b := range newValues {
				if tmp, err = common.DecodeInt64(b); err == nil {
					sum *= tmp
				}
			}
			res := new(bytes.Buffer)
			binary.Write(res, binary.BigEndian, sum)
			return [][]byte{res.Bytes()}
		}
	case common.FloatSum:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			var sum float64
			var tmp float64
			var err error
			for _, b := range oldValues {
				if tmp, err = common.DecodeFloat64(b); err == nil {
					sum += tmp
				}
			}
			for _, b := range newValues {
				if tmp, err = common.DecodeFloat64(b); err == nil {
					sum += tmp
				}
			}
			res := new(bytes.Buffer)
			binary.Write(res, binary.BigEndian, sum)
			return [][]byte{res.Bytes()}
		}
	case common.FloatSub:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			var sum float64
			var tmp float64
			var err error
			if tmp, err = common.DecodeFloat64(oldValues[0]); err == nil {
				sum = tmp
			}
			for _, b := range oldValues[1:] {
				if tmp, err = common.DecodeFloat64(b); err == nil {
					sum -= tmp
				}
			}
			for _, b := range newValues {
				if tmp, err = common.DecodeFloat64(b); err == nil {
					sum -= tmp
				}
			}
			res := new(bytes.Buffer)
			binary.Write(res, binary.BigEndian, sum)
			return [][]byte{res.Bytes()}
		}
	case common.FloatDiv:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			var sum float64
			var tmp float64
			var err error
			if tmp, err = common.DecodeFloat64(oldValues[0]); err == nil {
				sum = tmp
			}
			for _, b := range oldValues[1:] {
				if tmp, err = common.DecodeFloat64(b); err == nil {
					sum /= tmp
				}
			}
			for _, b := range newValues {
				if tmp, err = common.DecodeFloat64(b); err == nil {
					sum /= tmp
				}
			}
			res := new(bytes.Buffer)
			binary.Write(res, binary.BigEndian, sum)
			return [][]byte{res.Bytes()}
		}
	case common.FloatMul:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			var sum float64
			var tmp float64
			var err error
			if tmp, err = common.DecodeFloat64(oldValues[0]); err == nil {
				sum = tmp
			}
			for _, b := range oldValues[1:] {
				if tmp, err = common.DecodeFloat64(b); err == nil {
					sum *= tmp
				}
			}
			for _, b := range newValues {
				if tmp, err = common.DecodeFloat64(b); err == nil {
					sum *= tmp
				}
			}
			res := new(bytes.Buffer)
			binary.Write(res, binary.BigEndian, sum)
			return [][]byte{res.Bytes()}
		}
	case common.BigIntAnd:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			sum := new(big.Int).SetBytes(oldValues[0])
			for _, b := range oldValues[1:] {
				sum.And(sum, common.DecodeBigInt(b))
			}
			for _, b := range newValues {
				sum.And(sum, common.DecodeBigInt(b))
			}
			return [][]byte{sum.Bytes()}
		}
	case common.BigIntAdd:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			sum := new(big.Int).SetBytes(oldValues[0])
			for _, b := range oldValues[1:] {
				sum.Add(sum, common.DecodeBigInt(b))
			}
			for _, b := range newValues {
				sum.Add(sum, common.DecodeBigInt(b))
			}
			return [][]byte{sum.Bytes()}
		}
	case common.BigIntAndNot:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			sum := new(big.Int).SetBytes(oldValues[0])
			for _, b := range oldValues[1:] {
				sum.AndNot(sum, common.DecodeBigInt(b))
			}
			for _, b := range newValues {
				sum.AndNot(sum, common.DecodeBigInt(b))
			}
			return [][]byte{sum.Bytes()}
		}
	case common.BigIntDiv:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			sum := new(big.Int).SetBytes(oldValues[0])
			for _, b := range oldValues[1:] {
				sum.Div(sum, common.DecodeBigInt(b))
			}
			for _, b := range newValues {
				sum.Div(sum, common.DecodeBigInt(b))
			}
			return [][]byte{sum.Bytes()}
		}
	case common.BigIntMod:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			sum := new(big.Int).SetBytes(oldValues[0])
			for _, b := range oldValues[1:] {
				sum.Mod(sum, common.DecodeBigInt(b))
			}
			for _, b := range newValues {
				sum.Mod(sum, common.DecodeBigInt(b))
			}
			return [][]byte{sum.Bytes()}
		}
	case common.BigIntMul:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			sum := new(big.Int).SetBytes(oldValues[0])
			for _, b := range oldValues[1:] {
				sum.Mul(sum, common.DecodeBigInt(b))
			}
			for _, b := range newValues {
				sum.Mul(sum, common.DecodeBigInt(b))
			}
			return [][]byte{sum.Bytes()}
		}
	case common.BigIntOr:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			sum := new(big.Int).SetBytes(oldValues[0])
			for _, b := range oldValues[1:] {
				sum.Or(sum, common.DecodeBigInt(b))
			}
			for _, b := range newValues {
				sum.Or(sum, common.DecodeBigInt(b))
			}
			return [][]byte{sum.Bytes()}
		}
	case common.BigIntRem:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			sum := new(big.Int).SetBytes(oldValues[0])
			for _, b := range oldValues[1:] {
				sum.Rem(sum, common.DecodeBigInt(b))
			}
			for _, b := range newValues {
				sum.Rem(sum, common.DecodeBigInt(b))
			}
			return [][]byte{sum.Bytes()}
		}
	case common.BigIntSub:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			sum := new(big.Int).SetBytes(oldValues[0])
			for _, b := range oldValues[1:] {
				sum.Sub(sum, common.DecodeBigInt(b))
			}
			for _, b := range newValues {
				sum.Sub(sum, common.DecodeBigInt(b))
			}
			return [][]byte{sum.Bytes()}
		}
	case common.BigIntXor:
		return func(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
			sum := new(big.Int).SetBytes(oldValues[0])
			for _, b := range oldValues[1:] {
				sum.Xor(sum, common.DecodeBigInt(b))
			}
			for _, b := range newValues {
				sum.Xor(sum, common.DecodeBigInt(b))
			}
			return [][]byte{sum.Bytes()}
		}
	}
	panic(fmt.Errorf("Unknown SetOpType %v", int(m)))

}
