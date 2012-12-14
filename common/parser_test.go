package common

import (
	"reflect"
	"testing"
)

func TestSetOpParser(t *testing.T) {
	op, err := NewSetOpParser("(U (I c a (D f g)) (I c b) (X d e))").Parse()
	if err != nil {
		t.Error(err)
	}
	cmp := &SetOp{
		Type: Union,
		Sources: []interface{}{
			&SetOp{
				Type: Intersection,
				Sources: []interface{}{
					[]byte("c"),
					[]byte("a"),
					&SetOp{
						Type: Difference,
						Sources: []interface{}{
							[]byte("f"),
							[]byte("g"),
						},
					},
				},
			},
			&SetOp{
				Type: Intersection,
				Sources: []interface{}{
					[]byte("c"),
					[]byte("b"),
				},
			},
			&SetOp{
				Type: Xor,
				Sources: []interface{}{
					[]byte("d"),
					[]byte("e"),
				},
			},
		},
	}
	if !reflect.DeepEqual(op, cmp) {
		t.Errorf("%v and %v should be equal", op, cmp)
	}
}
