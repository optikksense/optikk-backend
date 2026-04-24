package ingest

import "sync"

// Pools centralises sync.Pool allocations shared across signal mappers and
// producers. The hot-path allocator in Go's runtime is fast, but at 200k
// records/s the per-row attribute maps and protobuf marshal buffers generate
// enough garbage to dominate the GC tax. Callers Acquire on the way in,
// Release before the value escapes the goroutine.
//
// All pools are deliberately narrowly typed — generic sync.Pool wrappers add
// a layer of `any` boxing that undoes the allocation savings.

// StringMapPool yields reset `map[string]string` values. Targeted at the
// attribute-string map each log/span row carries.
var StringMapPool = sync.Pool{
	New: func() any { return make(map[string]string, 16) },
}

// Float64MapPool yields reset `map[string]float64` values for numeric attrs.
var Float64MapPool = sync.Pool{
	New: func() any { return make(map[string]float64, 8) },
}

// BoolMapPool yields reset `map[string]bool` values for boolean attrs.
var BoolMapPool = sync.Pool{
	New: func() any { return make(map[string]bool, 4) },
}

// MarshalBufferPool yields reset `[]byte` scratch buffers backed by a 4 KiB
// initial cap. Used as the `b` in `proto.MarshalOptions{}.MarshalAppend(b, …)`
// so every marshal reuses the same backing array until the buffer grows.
var MarshalBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 4*1024)
		return &buf
	},
}

// AcquireStringMap pulls a reset map[string]string from the pool.
func AcquireStringMap() map[string]string {
	return StringMapPool.Get().(map[string]string)
}

// ReleaseStringMap clears the map and returns it to the pool. The compiler
// elides the range loop to runtime.mapclear (Go 1.21+) which is O(1) amortized.
func ReleaseStringMap(m map[string]string) {
	if m == nil {
		return
	}
	for k := range m {
		delete(m, k)
	}
	StringMapPool.Put(m)
}

// AcquireFloat64Map pulls a reset map[string]float64 from the pool.
func AcquireFloat64Map() map[string]float64 {
	return Float64MapPool.Get().(map[string]float64)
}

// ReleaseFloat64Map clears + returns.
func ReleaseFloat64Map(m map[string]float64) {
	if m == nil {
		return
	}
	for k := range m {
		delete(m, k)
	}
	Float64MapPool.Put(m)
}

// AcquireBoolMap pulls a reset map[string]bool from the pool.
func AcquireBoolMap() map[string]bool {
	return BoolMapPool.Get().(map[string]bool)
}

// ReleaseBoolMap clears + returns.
func ReleaseBoolMap(m map[string]bool) {
	if m == nil {
		return
	}
	for k := range m {
		delete(m, k)
	}
	BoolMapPool.Put(m)
}

// AcquireMarshalBuffer pulls a reset scratch buffer for protobuf marshal.
// Callers MUST copy the resulting bytes before release — the pool's backing
// array is reused by the next caller. Returning the buffer as a pointer lets
// the pool re-receive growths the caller made to the slice header.
func AcquireMarshalBuffer() *[]byte {
	return MarshalBufferPool.Get().(*[]byte)
}

// ReleaseMarshalBuffer resets the length and returns the pointer.
func ReleaseMarshalBuffer(b *[]byte) {
	if b == nil {
		return
	}
	*b = (*b)[:0]
	MarshalBufferPool.Put(b)
}
