package goworker

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"reflect"
	"testing"
	"time"
)

const (
	MasterSetName   = "resque"
	SentinelPort    = 26379
	FailoverTimeout = 1000 * time.Millisecond
)

func TestMasterFailover(t *testing.T) {
	setup, className, testKey, expectedPayloads := setupFailoverTest(t)
	defer func() {
		teardownFailoverTest(setup, t)
	}()

	registerWork(className, testKey, t, func() {
		pauseContainer(setup["master"][0].container, t)
	})

	response := readTestKey(testKey, setup, t)
	compareExpectedJobsToActual(response, expectedPayloads, t)
}

func TestSentinelFailover(t *testing.T) {
	setup, className, testKey, expectedPayloads := setupFailoverTest(t)
	defer func() {
		teardownFailoverTest(setup, t)
	}()

	registerWork(className, testKey, t, func() {
		pauseContainer(setup["master"][0].container, t)
		pauseContainer(setup["sentinel"][0].container, t)
		time.Sleep(3 * FailoverTimeout)
	})

	response := readTestKey(testKey, setup, t)
	compareExpectedJobsToActual(response, expectedPayloads, t)
}


func setupFailoverTest(t *testing.T) (containerSetup, string, string, []string) {
	setup := setupFailoverContainers(t)
	queueName, className, testKey := setupWorkerSettings(setup)
	expectedPayloads := scheduleJobs(queueName, className, t)

	return setup, className, testKey, expectedPayloads
}

func setupFailoverContainers(t *testing.T) containerSetup {
	dockerComposeUp(t)
	dockerComposeScaleSentinels(t)
	setup := scanSetup(t)
	verifySetupSize(setup, t)
	dockerComposeUnpause(setup, t)
	ensureMaster(setup, t)
	dockerClearDb(setup, t)

	return setup
}

func setupWorkerSettings(setup containerSetup) (string, string, string) {
	// config
	queueName := "failover_sentinel"
	className := "SomeRubyClass"
	testKey := fmt.Sprintf("test:%v", queueName)

	// setup worker
	workerSettings.URI = fmt.Sprintf(
		"redis+sentinel://%s:26379/resque",
		setup["sentinel"][0].address,
	)
	workerSettings.Queues = []string{queueName}
	workerSettings.UseNumber = true
	workerSettings.ExitOnComplete = true
	workerSettings.Concurrency = 1
	workerSettings.Connections = 1
	workerSettings.Timeout = FailoverTimeout / 2

	return queueName, className, testKey
}

func scheduleJobs(queueName, className string, t *testing.T) []string {
	// schedule a few jobs and store their payload
	expectedPayloads := []string{}
	for i := 1; i <= 5; i++ {
		err := Enqueue(&Job{
			Queue: queueName,
			Payload: Payload{
				Class: className,
				Args:  []interface{}{i},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		expectedPayloads = append(expectedPayloads, fmt.Sprint(i))
	}

	return expectedPayloads
}

func registerWork(className, testKey string, t *testing.T, middleWorker func()) {
	// register worker
	Register(className, func(queue string, args ...interface{}) error {
		// parse int payload
		val := args[0].(json.Number)
		int64Val, _ := val.Int64()
		intVal := int(int64Val)
		// in a separate connection store the payload in Redis under the test key
		func() {
			conn, err := GetConn()
			if err != nil {
				t.Fatal(err)
			}
			err = conn.Send("RPUSH", testKey, intVal)
			if err != nil {
				t.Fatal(err)
			}
			if err := conn.Flush(); err != nil {
				t.Fatal(err)
			}
			PutConn(conn)
		}()

		// kill master to force new connections from the pool and
		// kill primary sentinel to test auto discovery
		if intVal == 3 {
			middleWorker()
		}

		return nil
	})
	if err := Work(); err != nil {
		t.Fatal(err)
	}
}

func readTestKey(testKey string, setup containerSetup, t *testing.T) []string {
	// reconnect to a running Sentinel
	workerSettings.URI = fmt.Sprintf(
		"redis+sentinel://%s:26379/resque",
		setup["sentinel"][1].address,
	)
	if err := Init(); err != nil {
		t.Fatal(err)
	}

	conn, err := GetConn()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		PutConn(conn)
		Close()
	}()

	response, err := redis.Strings(conn.Do("LRANGE", testKey, 0, -1))
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func compareExpectedJobsToActual(response, expected []string, t *testing.T) {
	if !reflect.DeepEqual(response, expected) {
		t.Errorf("Expected jobs %v, got %v", expected, response)
	}
}

func teardownFailoverTest(setup containerSetup, t *testing.T) {
	dockerComposeUnpause(setup, t)
	dockerComposeStop(t)
}