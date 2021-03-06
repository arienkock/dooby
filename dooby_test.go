package dooby

import (
	"sync"
	"testing"
)

func TestDBSpan_Read(t *testing.T) {
	testDB := &DB{
		data: map[DBKey]DBValue{
			"A": "1",
		},
		Locker: new(sync.Mutex),
	}
	read := testDB.Start().Read("A")
	if read.Value != "1" {
		t.Errorf("bad read value. expected 1 got: %s", read.Value)
	}
}

func TestReadUncommittedValue(t *testing.T) {
	testDB := NewDB()
	firstRead := testDB.Start().Write("X", "Y").Read("X")
	if firstRead.Value != "Y" {
		t.Error("first read should be 'Y'")
	}
	secondRead := firstRead.Write("X", "Z").Read("X")
	if secondRead.Value != "Z" {
		t.Error("second read should be 'Z'")
	}
	if !secondRead.Commit().OK() {
		t.Error("uncommitted reads should not break congruency")
	}
}

func TestDBSpan_WriteAndCommit(t *testing.T) {
	testDB := &DB{
		data:   make(map[DBKey]DBValue),
		Locker: new(sync.Mutex),
	}
	if !testDB.Start().Write("A", "1").Commit().OK() {
		t.Error("Commit should be ok")
	}
	if testDB.data["A"] != "1" {
		t.Errorf("bad read value. expected 1 got: %s", testDB.data["A"])
	}
}

func TestDBSpan_IsCongruent(t *testing.T) {
	testDB := &DB{
		data: map[DBKey]DBValue{
			"A": "1",
			"B": "2",
		},
		Locker: new(sync.Mutex),
	}
	firstRead := testDB.Start().Read("A")
	if !firstRead.IsCongruent() {
		t.Error("read should be congruent")
	}
	parallelWrite := testDB.Start().Write("A", "3")
	if !parallelWrite.IsCongruent() {
		t.Error("parallelWrite should be congruent")
	}
	if !parallelWrite.Commit().OK() {
		t.Error("Commit should be ok")
	}
	if firstRead.IsCongruent() {
		t.Error("read should not be congruent after conflicting commit")
	}
	if firstRead.Commit().OK() {
		t.Error("commit on outdated read should not be ok")
	}
}

var benchmarkOK bool

func BenchmarkSerialWrites(b *testing.B) {
	var commitResult CommitResult
	for i := 0; i < b.N; i++ {
		commitResult = NewDB().Start().
			Write("A", "1").
			Write("A", "2").
			Write("A", "3").
			Write("A", "4").
			Write("A", "5").
			Commit()
	}
	benchmarkOK = commitResult.OK()
}

func BenchmarkSerialReadsAndWrites(b *testing.B) {
	var commitResult CommitResult
	for i := 0; i < b.N; i++ {
		commitResult = NewDB().Start().
			Write("A", "1").
			Read("A").
			Write("A", "2").
			Read("A").
			Write("A", "3").
			Read("A").
			Write("A", "4").
			Read("A").
			Write("A", "5").
			Read("A").
			Commit()
	}
	benchmarkOK = commitResult.OK()
}
