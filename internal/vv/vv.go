package vv

import "encoding/json"

type Vector map[string]uint64

type Order int

const (
	Equal Order = iota
	Before
	After
	Concurrent
)

func Compare(a, b Vector) Order {
	aBigger := false
	bBigger := false
	for id, n := range a {
		if n > b[id] {
			aBigger = true
		}
	}
	for id, n := range b {
		if n > a[id] {
			bBigger = true
		}
	}
	switch {
	case aBigger && bBigger:
		return Concurrent
	case aBigger:
		return After
	case bBigger:
		return Before
	default:
		return Equal
	}
}

func Merge(a, b Vector) Vector {
	result := Vector{}
	for id, n := range a {
		result[id] = n
	}
	for id, n := range b {
		if n > result[id] {
			result[id] = n
		}
	}
	return result
}

func Bump(v Vector, node string) Vector {
	result := Merge(v, nil)
	result[node]++
	return result
}

func Encode(v Vector) string {
	nonZero := Vector{}
	for id, n := range v {
		if n > 0 {
			nonZero[id] = n
		}
	}
	data, _ := json.Marshal(nonZero)
	return string(data)
}

func Decode(s string) (Vector, error) {
	v := Vector{}
	err := json.Unmarshal([]byte(s), &v)
	return v, err
}
