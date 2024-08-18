package main

import (
	"context"
	_ "embed"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed acquire.lua
var acquireLua string

//go:embed release.lua
var releaseLua string

type RemoteLock struct {
	client   *redis.Client
	acquired bool
	id       string
	key      string
}

func (l *RemoteLock) Acquire(timeout time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resp, err := l.client.Eval(ctx, acquireLua, []string{l.key}, []string{l.id, fmt.Sprintf("%d", timeout.Milliseconds())}).Result()

	if err != nil {
		return false, err
	}

	if resp != nil {
		reply, ok := resp.(string)
		if ok && reply == "OK" {
			l.acquired = true
			return true, nil
		}
	}
	return false, nil
}

func (l *RemoteLock) WaitAndAcquire(ctx context.Context, retryInterval time.Duration) error {
	for {
		locked, err := l.Acquire(1 * time.Second)
		if err != nil {
			return fmt.Errorf("failed to acquire lock: %v", err)
		}

		if locked {
			return nil
		}
		time.Sleep(retryInterval)
	}
}

func (l *RemoteLock) Release() error {
	if !l.acquired {
		return nil
	}
	ctx := context.Background()
	defer ctx.Done()

	resp, err := l.client.Eval(ctx, releaseLua, []string{l.key}, []string{l.id}).Result()

	if err != nil {
		return err
	}

	if resp != nil {
		reply, ok := resp.(int64)
		if ok && reply == 1 {
			l.acquired = false
			return nil
		}
	}

	return nil
}

func worker(id int, lockKey string, client *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()

	lock := RemoteLock{
		client: client,
		id:     fmt.Sprintf("worker-%d", id),
		key:    lockKey,
	}

	fmt.Printf("Worker %d: Trying to acquire lock...\n", id)

	err := lock.WaitAndAcquire(context.Background(), 500*time.Millisecond)
	if err != nil {
		fmt.Printf("Worker %d: %v\n", id, err)
		return
	}

	fmt.Printf("Worker %d: Acquired lock! Doing work...\n", id)
	// Simulate doing some work
	time.Sleep(time.Duration(rand.Intn(3)) * time.Second)

	err = lock.Release()
	if err != nil {
		fmt.Printf("Worker %d: Failed to release lock: %v\n", id, err)
		return
	}

	fmt.Printf("Worker %d: Released lock and done!\n", id)
}

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	// Key used for the distributed lock
	lockKey := "my-distributed-lock"

	var wg sync.WaitGroup

	// Start multiple workers
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go worker(i, lockKey, client, &wg)
	}

	wg.Wait()
	fmt.Println("All workers have finished.")
}
