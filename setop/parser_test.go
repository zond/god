package setop

import (
	"reflect"
	"testing"
)

func TestSetOpParser(t *testing.T) {
	op, err := NewSetOpParser("(U (I ccc aa (D ffff gg)) (I:ConCat c23 b_ff) (X dbla e&44))").Parse()
	if err != nil {
		t.Error(err)
	}
	cmp := &SetOp{
		Type: Union,
		Sources: []SetOpSource{
			SetOpSource{
				SetOp: &SetOp{
					Type: Intersection,
					Sources: []SetOpSource{
						SetOpSource{Key: []byte("ccc")},
						SetOpSource{Key: []byte("aa")},
						SetOpSource{
							SetOp: &SetOp{
								Type: Difference,
								Sources: []SetOpSource{
									SetOpSource{Key: []byte("ffff")},
									SetOpSource{Key: []byte("gg")},
								},
							},
						},
					},
				},
			},
			SetOpSource{
				SetOp: &SetOp{
					Type:  Intersection,
					Merge: ConCat,
					Sources: []SetOpSource{
						SetOpSource{Key: []byte("c23")},
						SetOpSource{Key: []byte("b_ff")},
					},
				},
			},
			SetOpSource{
				SetOp: &SetOp{
					Type: Xor,
					Sources: []SetOpSource{
						SetOpSource{Key: []byte("dbla")},
						SetOpSource{Key: []byte("e&44")},
					},
				},
			},
		},
	}
	if !reflect.DeepEqual(op, cmp) {
		t.Errorf("%v and %v should be equal", op, cmp)
	}
}
