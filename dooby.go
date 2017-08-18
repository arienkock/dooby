package dooby

import (
	"sync"
)

type DBKey string
type DBValue string
type CommitResult bool

func (cr CommitResult) OK() bool {
	return bool(cr)
}

type Record struct {
	Key         DBKey
	Value       DBValue
	Uncommitted bool
}

type DBSpan struct {
	db     *DB
	Parent *DBSpan
	Record
	IsRead bool
}

type noCopy struct{}

func (*noCopy) Lock() {}

type DB struct {
	noCopy noCopy
	data   map[DBKey]DBValue
	sync.Locker
}

func NewDB() *DB {
	return &DB{
		data:   make(map[DBKey]DBValue),
		Locker: new(sync.Mutex),
	}
}

func (d *DB) Start() *DBSpan {
	return &DBSpan{
		db: d,
	}
}

func (d DBSpan) IsCongruent() bool {
	currentSpan := &d
	for currentSpan != nil {
		if currentSpan.IsRead && !currentSpan.Uncommitted && d.db.data[currentSpan.Key] != currentSpan.Value {
			return false
		}
		currentSpan = currentSpan.Parent
	}
	return true
}

func (d *DBSpan) Read(key DBKey) *DBSpan {
	var foundInSpan bool
	var foundValue DBValue
	currentSpan := d
	for currentSpan != nil {
		if !currentSpan.IsRead && currentSpan.Key == key {
			foundValue = currentSpan.Value
			foundInSpan = true
			break
		}
		currentSpan = currentSpan.Parent
	}
	if !foundInSpan {
		foundValue = d.db.data[key]
	}
	return &DBSpan{
		db:     d.db,
		Parent: d,
		IsRead: true,
		Record: Record{
			Key:         key,
			Value:       foundValue,
			Uncommitted: foundInSpan,
		},
	}
}

func (d *DBSpan) Write(key DBKey, val DBValue) *DBSpan {
	return &DBSpan{
		db:     d.db,
		Parent: d,
		Record: Record{
			Key:   key,
			Value: val,
		},
	}
}

func (d DBSpan) Commit() CommitResult {
	d.db.Lock()
	result := CommitResult(false)
	if d.IsCongruent() {
		result = d.commit()
	}
	d.db.Unlock()
	return result
}

func (d DBSpan) commit() CommitResult {
	parentResult := CommitResult(true)
	if d.Parent != nil {
		parentResult = d.Parent.commit()
	}
	if !parentResult.OK() {
		return parentResult
	}
	if d.IsRead && d.db.data[d.Key] != d.Value {
		return false
	}
	if !d.IsRead {
		d.db.data[d.Key] = d.Value
	}
	return true
}
