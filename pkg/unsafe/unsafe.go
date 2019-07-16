package unsafe

import (
	"fmt"
	"reflect"
	"unsafe"
)

type iptr struct {
	itab uintptr
	ptr  uintptr
}

func (p iptr) String() string {
	return fmt.Sprintf("itab=0x%x, ptr=0x%x", p.itab, p.ptr)
}

func (p iptr) Interface() interface{} {
	return *(*interface{})(unsafe.Pointer(&p))
}

type InterfaceFactory struct {
	ptritab unsafe.Pointer
}

func NewInterfaceFactory(t reflect.Type) InterfaceFactory {
	// Need something valid to point at, just a throwaway
	tmp := struct{}{}
	ptr := unsafe.Pointer(&tmp)

	// Construct a pointer of type *t that points at tmp
	ptrPtrVal := reflect.NewAt(t, ptr)

	// Build interface{*t, ptrPtrVal=>tmp}
	ptrPtrIface := ptrPtrVal.Interface()

	// Coerce the above interface{} into a touchable struct
	ptrPtrIptr := *(*iptr)(unsafe.Pointer(&ptrPtrIface))

	// All we care about is the itab field, which contains the
	// type information to copy onto factory-created interface{}s
	itabPtr := unsafe.Pointer(ptrPtrIptr.itab)

	return InterfaceFactory{
		ptritab: itabPtr,
	}
}

// takes interface{Struct, ptr=>struct}, returns interface{*FieldType, ptr=>&struct.field}
func (f *InterfaceFactory) PtrToStructField(any interface{}, field reflect.StructField) interface{} {
	// creates an iptr struct out of the 'any' interface
	anyIptr := *(*iptr)(unsafe.Pointer(&any))

	// construct the new pointer (&struct.field) that will be returned
	interptr := unsafe.Pointer(anyIptr.ptr + field.Offset)

	// create a new iptr by copying the template iptr, which has the proper type
	newIptr := iptr{
		itab: uintptr(f.ptritab),
		ptr:  uintptr(interptr),
	}

	return newIptr.Interface()
}
