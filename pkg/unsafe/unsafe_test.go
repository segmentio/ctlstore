// +build !race

package unsafe

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestInterfaceFactoryPtrToStructField(t *testing.T) {
	myStruct := struct{ X, Y string }{"hello", "world"}
	myStructX := reflect.TypeOf(myStruct).Field(0)
	myStructY := reflect.TypeOf(myStruct).Field(1)
	xif := NewInterfaceFactory(myStructX.Type)
	yif := NewInterfaceFactory(myStructY.Type)

	xptr := xif.PtrToStructField(&myStruct, myStructX)
	yptr := yif.PtrToStructField(&myStruct, myStructY)

	t.Logf("xptr=%v:%v, yptr=%v:%v\n",
		xptr,
		reflect.TypeOf(xptr),
		yptr,
		reflect.TypeOf(yptr))

	*(xptr.(*string)) = "goodbye"
	*(yptr.(*string)) = "earth"

	if diff := cmp.Diff(myStruct, struct{ X, Y string }{"goodbye", "earth"}); diff != "" {
		t.Errorf("Mismatch struct\n%v", diff)
	}
}
