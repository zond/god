package setop

import (
  "bytes"
  "encoding/binary"
  "fmt"
  "github.com/zond/god/common"
  "math/big"
)

type mergeFunc func(oldValues [][]byte, newValues [][]byte) (result [][]byte)

func getMerger(m SetOpMerge) mergeFunc {
  switch m {
  case Append:
    return _append
  case ConCat:
    return conCat
  case IntegerSum:
    return integerSum
  case IntegerSub:
    return integerSub
  case IntegerDiv:
    return integerDiv
  case IntegerMul:
    return integerMul
  case FloatSum:
    return floatSum
  case FloatSub:
    return floatSub
  case FloatDiv:
    return floatDiv
  case FloatMul:
    return floatMul
  case BigIntAnd:
    return bigIntAnd
  case BigIntAdd:
    return bigIntAnd
  case BigIntAndNot:
    return bigIntAndNot
  case BigIntDiv:
    return bigIntDiv
  case BigIntMod:
    return bigIntMod
  case BigIntMul:
    return bigIntMul
  case BigIntOr:
    return bigIntOr
  case BigIntRem:
    return bigIntRem
  case BigIntSub:
    return bigIntSub
  case BigIntXor:
    return bigIntXor
  case First:
    return first
  case Last:
    return last
  }
  panic(fmt.Errorf("Unknown SetOpType %v", int(m)))
}

func last(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
  return [][]byte{newValues[len(newValues)-1]}
}
func first(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
  return [][]byte{oldValues[0]}
}
func _append(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
  return append(oldValues, newValues...)
}
func conCat(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
  var res []byte
  for _, b := range oldValues {
    res = append(res, b...)
  }
  for _, b := range newValues {
    res = append(res, b...)
  }
  return [][]byte{res}
}
func integerSum(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
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
func integerSub(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
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
func integerDiv(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
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
func integerMul(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
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
func floatSum(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
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
func floatSub(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
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
func floatDiv(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
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
func floatMul(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
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
func bigIntAnd(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
  sum := new(big.Int).SetBytes(oldValues[0])
  for _, b := range oldValues[1:] {
    sum.And(sum, common.DecodeBigInt(b))
  }
  for _, b := range newValues {
    sum.And(sum, common.DecodeBigInt(b))
  }
  return [][]byte{sum.Bytes()}
}
func bigIntAdd(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
  sum := new(big.Int).SetBytes(oldValues[0])
  for _, b := range oldValues[1:] {
    sum.Add(sum, common.DecodeBigInt(b))
  }
  for _, b := range newValues {
    sum.Add(sum, common.DecodeBigInt(b))
  }
  return [][]byte{sum.Bytes()}
}
func bigIntAndNot(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
  sum := new(big.Int).SetBytes(oldValues[0])
  for _, b := range oldValues[1:] {
    sum.AndNot(sum, common.DecodeBigInt(b))
  }
  for _, b := range newValues {
    sum.AndNot(sum, common.DecodeBigInt(b))
  }
  return [][]byte{sum.Bytes()}
}
func bigIntDiv(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
  sum := new(big.Int).SetBytes(oldValues[0])
  for _, b := range oldValues[1:] {
    sum.Div(sum, common.DecodeBigInt(b))
  }
  for _, b := range newValues {
    sum.Div(sum, common.DecodeBigInt(b))
  }
  return [][]byte{sum.Bytes()}
}
func bigIntMod(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
  sum := new(big.Int).SetBytes(oldValues[0])
  for _, b := range oldValues[1:] {
    sum.Mod(sum, common.DecodeBigInt(b))
  }
  for _, b := range newValues {
    sum.Mod(sum, common.DecodeBigInt(b))
  }
  return [][]byte{sum.Bytes()}
}
func bigIntMul(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
  sum := new(big.Int).SetBytes(oldValues[0])
  for _, b := range oldValues[1:] {
    sum.Mul(sum, common.DecodeBigInt(b))
  }
  for _, b := range newValues {
    sum.Mul(sum, common.DecodeBigInt(b))
  }
  return [][]byte{sum.Bytes()}
}
func bigIntOr(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
  sum := new(big.Int).SetBytes(oldValues[0])
  for _, b := range oldValues[1:] {
    sum.Or(sum, common.DecodeBigInt(b))
  }
  for _, b := range newValues {
    sum.Or(sum, common.DecodeBigInt(b))
  }
  return [][]byte{sum.Bytes()}
}
func bigIntRem(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
  sum := new(big.Int).SetBytes(oldValues[0])
  for _, b := range oldValues[1:] {
    sum.Rem(sum, common.DecodeBigInt(b))
  }
  for _, b := range newValues {
    sum.Rem(sum, common.DecodeBigInt(b))
  }
  return [][]byte{sum.Bytes()}
}
func bigIntXor(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
  sum := new(big.Int).SetBytes(oldValues[0])
  for _, b := range oldValues[1:] {
    sum.Xor(sum, common.DecodeBigInt(b))
  }
  for _, b := range newValues {
    sum.Xor(sum, common.DecodeBigInt(b))
  }
  return [][]byte{sum.Bytes()}
}
func bigIntSub(oldValues [][]byte, newValues [][]byte) (result [][]byte) {
  sum := new(big.Int).SetBytes(oldValues[0])
  for _, b := range oldValues[1:] {
    sum.Sub(sum, common.DecodeBigInt(b))
  }
  for _, b := range newValues {
    sum.Sub(sum, common.DecodeBigInt(b))
  }
  return [][]byte{sum.Bytes()}
}
