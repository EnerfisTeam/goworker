package goworker

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
	"github.com/garyburd/redigo/redis"
	"reflect"
)

const (
	MasterSetName   = "resque"
	SentinelPort    = 26379
	FailoverTimeout = 1000 * time.Millisecond
)

func TestMasterFailover(t *testing.T) {
	dockerComposeUp(t)
	dockerComposeScaleSentinels(t)
	setup := scanSetup(t)
	verifySetupSize(setup, t)
	dockerComposeUnpause(setup, t)
	ensureMaster(setup, t)
	dockerClearDb(setup, t)
	defer func() {
		dockerComposeUnpause(setup, t)
		dockerComposeStop(t)
	}()

	// config
	queueName := "failover_test"
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

	// register worker
	Register(className, func(queue string, args ...interface{}) error {
		// parse int payload
		val := args[0].(json.Number)
		int64Val, _ := val.Int64()
		intVal := int(int64Val)

		// in a separate connection store the payload in Redis under the test key
		func () {
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

		// kill master
		if intVal == 3 {
			pauseContainer(setup["master"][0].container, t)
		}

		return nil
	})
	if err := Work(); err != nil {
		t.Fatal(err)
	}

	// check test keys
	response := func() []string {
		if err := Init(); err != nil {
			t.Fatal(err)
		}

		conn, err := GetConn()
		if err != nil {
			t.Fatal(err)
		}
		defer func(){
			PutConn(conn)
			Close()
		}()

		response, err := redis.Strings(conn.Do("LRANGE", testKey, 0, -1))
		if err != nil {
			t.Fatal(err)
		}
		return response
	}()

	// verify that all jobs were processed and the latest master knows about it
	if !reflect.DeepEqual(response, expectedPayloads) {
		t.Errorf("Expected jobs %v, got %v", expectedPayloads, response)
	}
}