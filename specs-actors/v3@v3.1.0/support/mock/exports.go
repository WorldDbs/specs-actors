package mock

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/ipfs/go-cid"
)

func CheckActorExports(t *testing.T, act interface{ Exports() []interface{} }) {
	for i, m := range act.Exports() {
		if i == 0 { // Send is implicit
			continue
		}

		if m == nil {
			continue
		}

		t.Run(fmt.Sprintf("method%d-type", i), func(t *testing.T) {
			mrt := Runtime{t: t}
			mrt.verifyExportedMethodType(reflect.ValueOf(m))
		})

		t.Run(fmt.Sprintf("method%d-unsafe-input", i), func(t *testing.T) {
			paramsType := reflect.ValueOf(m).Type().In(1)
			checkUnsafeInputs(t, paramsType.String(), paramsType)
		})
	}
}

var tCID = reflect.TypeOf(new(cid.Cid)).Elem()

func checkUnsafeInputs(t *testing.T, name string, typ reflect.Type) {
	switch typ.Kind() {
	case reflect.Array:
		fallthrough
	case reflect.Slice:
		fallthrough
	case reflect.Map:
		fallthrough
	case reflect.Ptr:
		checkUnsafeInputs(t, name+".elem", typ.Elem())

	case reflect.Struct:
		if typ == tCID {
			t.Fatal("method has unchecked CID input at ", name)
		}

		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)

			if f.Tag.Get("checked") == "true" {
				if f.Type != tCID {
					t.Fatal("expected checked value to be cid.Cid")
				}

				continue
			}

			checkUnsafeInputs(t, name+"."+f.Name, f.Type)
		}

	case reflect.Interface:
		t.Fatal("method has unsafe interface{} input parameter")
	}
}
