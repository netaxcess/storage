package main
  
import (
	
	"fmt"
	"time"
    
   "github.com/netaxcess/util/btree"
   "github.com/netaxcess/timewheel"
)



var (
	defaultTW = timewheel.NewTimeoutWheel(timewheel.WithTickInterval(time.Millisecond * 50))
)

// DefaultTimeoutWheel returns default timeout wheel
func DefaultTimeoutWheel() *timewheel.TimeoutWheel {
	return defaultTW
}


// Storage memory storage
type Storage struct {
	kv *btree.KVTree
}

// NewStorage returns a mem data storage
func NewStorage() *Storage {
	return &Storage{
		kv: btree.NewKVTree(),
	}
}

// SetWithTTL put the key, value pair to the storage with a ttl in seconds
func (s *Storage) SetWithTTL(key []byte, value []byte, ttl int32) error {
	s.kv.Put(key, value)
	if ttl > 0 {
		after := time.Second * time.Duration(ttl)
		DefaultTimeoutWheel().Schedule(after, func(arg interface{}) {
			s.Delete(arg.([]byte))
		}, key)
	}
	return nil
}

// Set put the key, value pair to the storage
func (s *Storage) Set(key []byte, value []byte) error {
	return s.SetWithTTL(key, value, 0)
}


// BatchSet batch set
func (s *Storage) BatchSet(pairs ...[]byte) error {
	if len(pairs)%2 != 0 {
		return fmt.Errorf("invalid args len: %d", len(pairs))
	}

	for i := 0; i < len(pairs)/2; i++ {
		s.Set(pairs[2*i], pairs[2*i+1])
	}

	return nil
}

// Get returns the value of the key
func (s *Storage) Get(key []byte) ([]byte, error) {
	v := s.kv.Get(key)
	return v, nil
}

// MGet returns multi values
func (s *Storage) MGet(keys ...[]byte) ([][]byte, error) {
	var values [][]byte
	for _, key := range keys {
		values = append(values, s.kv.Get(key))
	}

	return values, nil
}

// Delete remove the key from the storage
func (s *Storage) Delete(key []byte) error {
	s.kv.Delete(key)
	return nil
}

// BatchDelete batch delete
func (s *Storage) BatchDelete(keys ...[]byte) error {
	for _, key := range keys {
		s.kv.Delete(key)
	}

	return nil
}

// RangeDelete remove data in [start,end)
func (s *Storage) RangeDelete(start, end []byte) error {
	s.kv.RangeDelete(start, end)
	return nil
}

// Scan scans the key-value paire in [start, end), and perform with a handler function, if the function
// returns false, the scan will be terminated, if the `pooledKey` is true, raftstore will call `Free` when
// scan completed.
func (s *Storage) Scan(start, end []byte, handler func(key, value []byte) (bool, error), pooledKey bool) error {
	return s.kv.Scan(start, end, handler)
}

// Free free the pooled bytes
func (s *Storage) Free(pooled []byte) {

}

// SplitCheck Find a key from [start, end), so that the sum of bytes of the value of [start, key) <=size,
// returns the current bytes in [start,end), and the founded key
func (s *Storage) SplitCheck(start []byte, end []byte, size uint64) (uint64, []byte, error) {
	total := uint64(0)
	found := false
	var splitKey []byte
	s.kv.Scan(start, end, func(key, value []byte) (bool, error) {
		total += uint64(len(key) + len(value))
		if !found && total >= size {
			found = true
			splitKey = key
		}
		return true, nil
	})

	return total, splitKey, nil
}

// Seek returns the first key-value that >= key
func (s *Storage) Seek(key []byte) ([]byte, []byte, error) {
	k, v := s.kv.Seek(key)
	return k, v, nil
}



// Close close the storage
func (s *Storage) Close() error {
	return nil
}


func main() {
    s := NewStorage()
    s.Set([]byte("L001"), []byte("我操1"))
    s.Set([]byte("L002"), []byte("我操2"))
    s.Set([]byte("L004"), []byte("我操3"))
    s.Set([]byte("L003"), []byte("我操4"))
    s.Set([]byte("L005"), []byte("我操5"))
    //v, _ := s.MGet([]byte("a"), []byte("b"), []byte("c"))
    //for _ , vv := range v {
        //fmt.Println(string(vv))
    //}

	s.Scan([]byte("L001"), []byte("L005"), func(key, value []byte) (bool, error) {
		fmt.Println(string(key),"==",string(value))
		
		return true, nil
	}, true)
}