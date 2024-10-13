package store

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/boltdb/bolt"
	"orchestrator/task"
)

type BoltDBTaskStore struct {
	db *bolt.DB

	dbFile string

	fileMode os.FileMode

	bucketName string
}

func NewBoltDBTaskStore(file string, mode os.FileMode, bucketName string) (*BoltDBTaskStore, error) {
	db, err := bolt.Open(file, mode, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to open %v", file)
	}

	taskStore := BoltDBTaskStore{
		db:         db,
		dbFile:     file,
		fileMode:   mode,
		bucketName: bucketName,
	}

	err = taskStore.CreateBucket()
	if err != nil {
		log.Printf("bucket already exists, will use it instead of creating new one")
	}

	return &taskStore, nil
}

func (s *BoltDBTaskStore) Put(key string, value interface{}) error {
	return s.db.Update(
		func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(s.bucketName))

			buf, err := json.Marshal(value.(*task.Task))
			if err != nil {
				return err
			}

			err = bucket.Put([]byte(key), buf)
			if err != nil {
				return err
			}

			return nil
		},
	)
}

func (s *BoltDBTaskStore) Get(key string) (interface{}, error) {
	var _task task.Task

	err := s.db.View(
		func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(s.bucketName))

			value := bucket.Get([]byte(key))
			if value == nil {
				return fmt.Errorf("task %v not found", key)
			}

			err := json.Unmarshal(value, &_task)
			if err != nil {
				return err
			}

			return nil
		},
	)

	if err != nil {
		return nil, err
	}

	return &_task, nil
}

func (s *BoltDBTaskStore) List() (interface{}, error) {
	var tasks []*task.Task

	err := s.db.View(
		func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(s.bucketName))

			bucket.ForEach(
				func(k, v []byte) error {
					var _task task.Task
					err := json.Unmarshal(v, &_task)

					if err != nil {
						return err
					}

					tasks = append(tasks, &_task)

					return nil
				},
			)

			return nil
		},
	)

	if err != nil {
		return nil, err
	}

	return tasks, nil
}

func (s *BoltDBTaskStore) Count() (int, error) {
	taskCount := 0

	err := s.db.View(
		func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("tasks"))

			bucket.ForEach(
				func(k, v []byte) error {
					taskCount++
					return nil
				},
			)

			return nil
		},
	)

	if err != nil {
		return -1, err
	}

	return taskCount, nil
}

func (s *BoltDBTaskStore) CreateBucket() error {
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

func (s *BoltDBTaskStore) Close() {
	s.db.Close()
}
