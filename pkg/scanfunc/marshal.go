package scanfunc

import (
	"errors"
	"reflect"
	"sync"

	"github.com/segmentio/ctlstore/pkg/unsafe"
)

type (
	UnmarshalMetaCache struct {
		cache map[reflect.Type]UnmarshalTypeMeta
		mu    sync.RWMutex
	}
	UnmarshalTypeMeta struct {
		Fields map[string]UnmarshalTypeMetaField
	}
	UnmarshalTypeMetaField struct {
		Field   reflect.StructField
		Factory unsafe.InterfaceFactory
	}
	UtmGetterFunc func(reflect.Type) (UnmarshalTypeMeta, error)
)

var ErrUnmarshalUnsupportedType = errors.New("only map[string]interface{} and struct pointer types are supported for unmarshalling")
var UtcNoopScanner interface{} = NoOpScanner{}
var UtcCache = UnmarshalMetaCache{cache: map[reflect.Type]UnmarshalTypeMeta{}}

func (umc *UnmarshalMetaCache) GetOrSet(typ reflect.Type, getter UtmGetterFunc) (UnmarshalTypeMeta, error) {
	umc.mu.RLock()
	defer umc.mu.RUnlock()

	if meta, ok := umc.cache[typ]; ok {
		return meta, nil
	}

	umc.mu.RUnlock()
	umc.mu.Lock()

	// There's a race here between the RUnlock and Lock, since another writer
	// could be waiting for write lock ahead of the line of this writer. That's
	// ok though because collecting this information isn't super expensive, and
	// the getter will always return the same thing for a type. The program
	// will quickly map out all the types used and this becomes basically
	// fixed.

	meta, err := getter(typ)
	if err == nil {
		umc.cache[typ] = meta
	}

	umc.mu.Unlock()
	umc.mu.RLock()
	return meta, err
}

func (umc *UnmarshalMetaCache) Invalidate(typ reflect.Type) {
	umc.mu.Lock()
	defer umc.mu.Unlock()
	delete(umc.cache, typ)
}
