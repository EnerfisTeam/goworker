package goworker

import (
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/context"

	"errors"
	"github.com/cihub/seelog"
)

var (
	logger      seelog.LoggerInterface
	sentinel    *Sentinel
	ctx         context.Context
	initMutex   sync.Mutex
	initialized bool
)

var workerSettings WorkerSettings

type WorkerSettings struct {
	QueuesString      string
	Queues            queuesFlag
	IntervalFloat     float64
	Interval          intervalFlag
	Concurrency       int
	Connections       int
	URI               string
	Namespace         string
	ExitOnComplete    bool
	IsStrict          bool
	UseNumber         bool
	Timeout           time.Duration
	ConnectionRetries int
}

func SetSettings(settings WorkerSettings) {
	workerSettings = settings
}

// Init initializes the goworker process. This will be
// called by the Work function, but may be used by programs
// that wish to access goworker functions and configuration
// without actually processing jobs.
func Init() error {
	initMutex.Lock()
	defer initMutex.Unlock()
	if !initialized {
		var err error
		logger, err = seelog.LoggerFromWriterWithMinLevel(os.Stdout, seelog.InfoLvl)
		if err != nil {
			return err
		}

		if err := flags(); err != nil {
			return err
		}
		ctx = context.Background()

		sentinel, err = NewSentinel(
			workerSettings.URI,
			workerSettings.Connections,
			workerSettings.Connections,
			workerSettings.Timeout,
		)
		if err != nil {
			return err
		}
		go func() {
			for {
				if err := sentinel.Discover(); err != nil {
					logger.Errorf("Sentinel discovery failed with %v", err)
				}
				time.Sleep(time.Minute)
			}
		}()

		if err != nil {
			return err
		}

		initialized = true
	}
	return nil
}

// GetConn returns a connection from the goworker Redis
// connection pool. When using the pool, check in
// connections as quickly as possible, because holding a
// connection will cause concurrent worker functions to
// lock while they wait for an available connection.
//
// The connection pool holds connections to a specific
// master which might go down or be demoted to slave. If
// either happens, the connection is closed and removed
// from the pool which will provide a brand new connection
// when asked. GetConn tries to get a new connection
// several times and only if no attempt succeeds, it
// returns the error.
func GetConn() (*RedisConn, error) {
	deadConnection := errors.New("Dead connection")
	slaveConnection := errors.New("Stale connection (to slave, not master)")
	try := func() (*RedisConn, error) {
		conn, err := sentinel.GetConn(ctx)
		if err != nil {
			return nil, err
		}

		// close the connection and remove it from the pool so that new
		// connections get created

		// if the instance does not ping back
		_, err = conn.Do("ping")
		if err != nil {
			conn.Close()
			PutConn(nil)
			return nil, deadConnection
		}

		// or if the instance is not a master anxmore
		role, err := role(conn.Do("role"))
		if err != nil {
			return nil, err
		}
		if role != "master" {
			conn.Close()
			PutConn(nil)
			return nil, slaveConnection
		}
		return conn, nil
	}

	var conn *RedisConn
	var err error
	for i := 0; i < workerSettings.ConnectionRetries; i++ {
		if conn, err = try(); err == nil {
			return conn, nil
		} else if err != slaveConnection && err != deadConnection {
			return nil, err
		}
	}

	return conn, err
}

// PutConn puts a connection back into the connection pool.
// Run this as soon as you finish using a connection that
// you got from GetConn. Expect this API to change
// drastically.
func PutConn(conn *RedisConn) {
	sentinel.PutConn(conn)
}

// Close cleans up resources initialized by goworker. This
// will be called by Work when cleaning up. However, if you
// are using the Init function to access goworker functions
// and configuration without processing jobs by calling
// Work, you should run this function when cleaning up. For
// example,
//
//	if err := goworker.Init(); err != nil {
//		fmt.Println("Error:", err)
//	}
//	defer goworker.Close()
func Close() {
	initMutex.Lock()
	defer initMutex.Unlock()
	if initialized {
		sentinel.Close()
		initialized = false
	}
}

// Work starts the goworker process. Check for errors in
// the return value. Work will take over the Go executable
// and will run until a QUIT, INT, or TERM signal is
// received, or until the queues are empty if the
// -exit-on-complete flag is set.
func Work() error {
	err := Init()
	if err != nil {
		return err
	}
	defer Close()

	quit := signals()

	poller, err := newPoller(workerSettings.Queues, workerSettings.IsStrict)
	if err != nil {
		return err
	}
	jobs := poller.poll(time.Duration(workerSettings.Interval), quit)

	var monitor sync.WaitGroup

	for id := 0; id < workerSettings.Concurrency; id++ {
		worker, err := newWorker(strconv.Itoa(id), workerSettings.Queues)
		if err != nil {
			return err
		}
		worker.work(jobs, &monitor)
	}

	monitor.Wait()

	return nil
}
