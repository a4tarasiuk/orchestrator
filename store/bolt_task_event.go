package store

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/boltdb/bolt"
	"orchestrator/task"
)

type BoltDBTaskEventStore struct {
	db *bolt.DB

	dbFile string

	fileMode os.FileMode

	bucketName string
}

func NewBoltDbTaskEventStore(file string, mode os.FileMode, bucketName string) (*BoltDBTaskEventStore, error) {
	db, err := bolt.Open(file, mode, nil)

	if err != nil {
		return nil, fmt.Errorf("unable to open %v", file)
	}
	e := BoltDBTaskEventStore{
		db:         db,
		dbFile:     file,
		fileMode:   mode,
		bucketName: bucketName,
	}
	err = e.CreateBucket()

	if err != nil {
		log.Printf("bucket '%s' already exists, will use it instead of creating new one", e.bucketName)
	}

	return &e, nil
}

func (s *BoltDBTaskEventStore) Put(key string, value interface{}) error {
	return s.db.Update(
		func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(s.bucketName))

			buf, err := json.Marshal(value.(*task.TaskEvent))

			if err != nil {
				return err
			}

			err = bucket.Put([]byte(key), buf)

			if err != nil {
				log.Printf("unable to save item %s", key)
				return err
			}

			return nil
		},
	)
}

func (s *BoltDBTaskEventStore) Get(key string) (interface{}, error) {
	var event task.TaskEvent

	err := s.db.View(
		func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(s.bucketName))

			value := bucket.Get([]byte(key))

			if value == nil {
				return fmt.Errorf("event %v not found", key)
			}

			err := json.Unmarshal(value, &event)

			if err != nil {
				return err
			}

			return nil
		},
	)

	if err != nil {
		return nil, err
	}

	return &event, nil
}

func (s *BoltDBTaskEventStore) List() (interface{}, error) {
	var events []*task.TaskEvent

	err := s.db.View(
		func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(s.bucketName))

			bucket.ForEach(
				func(k, v []byte) error {
					var event task.TaskEvent

					err := json.Unmarshal(v, &event)

					if err != nil {
						return err
					}

					events = append(events, &event)

					return nil
				},
			)

			return nil
		},
	)

	if err != nil {
		return nil, err
	}

	return events, nil
}

func (s *BoltDBTaskEventStore) Count() (int, error) {
	eventCount := 0

	err := s.db.View(
		func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(s.bucketName))

			bucket.ForEach(
				func(k, v []byte) error {
					eventCount++
					return nil
				},
			)

			return nil
		},
	)

	if err != nil {
		return -1, err
	}

	return eventCount, nil
}

func (s *BoltDBTaskEventStore) CreateBucket() error {
	return s.db.Update(
		func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket([]byte(s.bucketName))

			if err != nil {
				return fmt.Errorf("create bucket %s: %s", s.bucketName, err)
			}

			return nil
		},
	)
}

func (s *BoltDBTaskEventStore) Close() {
	s.db.Close()
}
