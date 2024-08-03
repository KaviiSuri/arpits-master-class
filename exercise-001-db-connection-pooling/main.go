package main

import (
	"database/sql"
	"log/slog"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type ConnPool struct {
	conns chan *sql.DB
}

const NumRoutines = 10000
const dbUrl = "root:secret@tcp(127.0.0.1:3306)/testdb"

func NewConnPool(n int) *ConnPool {
	p := &ConnPool{
		conns: make(chan *sql.DB, n),
	}

	for i := 0; i < n; i++ {
		conn, err := sql.Open("mysql", dbUrl)
		if err != nil {
			slog.Error("Failed to open database connection", "error", err)
			os.Exit(1)
		}
		p.conns <- conn
	}

	return p
}

func (p *ConnPool) Get() *sql.DB {
	slog.Debug("Get attempt")
	conn := <-p.conns
	slog.Debug("Got connection")
	return conn
}

func (p *ConnPool) Put(conn *sql.DB) {
	slog.Debug("Put attempt")
	select {
	case p.conns <- conn:
		slog.Debug("Connection returned to pool")
	default:
		conn.Close()
		slog.Debug("Connection discarded")
	}
}

func (p *ConnPool) Close() {
	close(p.conns)
	for conn := range p.conns {
		conn.Close()
	}
}

func BenchmarkConnPool() {
	startTime := time.Now()
	p := NewConnPool(10)
	defer p.Close()
	slog.Debug("Connection pool created")

	wg := sync.WaitGroup{}

	for i := 0; i < NumRoutines; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			conn := p.Get()
			defer p.Put(conn)

			_, err := conn.Exec("SELECT SLEEP(1.0)")
			if err != nil {
				slog.Error("Error executing query", "error", err)
			}
		}(&wg)
	}
	wg.Wait()
	elapsedTime := time.Since(startTime)
	slog.Info("Benchmark pool completed", "duration", elapsedTime)
}

func BenchmarkDirectConn() {
	startTime := time.Now()
	wg := sync.WaitGroup{}

	for i := 0; i < NumRoutines; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			conn, err := sql.Open("mysql", dbUrl)
			if err != nil {
				slog.Error("Failed to open database connection", "error", err)
				os.Exit(1)
			}
			defer conn.Close()

			_, err = conn.Exec("SELECT SLEEP(1.0)")
			if err != nil {
				slog.Error("Error executing query", "error", err)
			}
		}(&wg)
	}
	wg.Wait()
	elapsedTime := time.Since(startTime)
	slog.Info("Benchmark direct completed", "duration", elapsedTime)
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
	BenchmarkConnPool()
	BenchmarkDirectConn()
}
