package goworker

import (
	"reflect"
	"testing"
	"time"
	"fmt"
)

var workerMarshalJSONTests = []struct {
	w        worker
	expected []byte
}{
	{
		worker{},
		[]byte(`":0-:"`),
	},
	{
		worker{
			process: process{
				Hostname: "hostname",
				Pid:      12345,
				ID:       "123",
				Queues:   []string{"high", "low"},
			},
		},
		[]byte(`"hostname:12345-123:high,low"`),
	},
}

func TestWorkerMarshalJSON(t *testing.T) {
	for _, tt := range workerMarshalJSONTests {
		actual, err := tt.w.MarshalJSON()
		if err != nil {
			t.Errorf("Worker(%#v): error %s", tt.w, err)
		} else {
			if string(actual) != string(tt.expected) {
				t.Errorf("Worker(%#v): expected %s, actual %s", tt.w, tt.expected, actual)
			}
		}
	}
}

func TestConnectionString(t *testing.T) {
	for _, tt := range []struct {
		uri           string
		expectedError error
	}{
		{
			uri: "redis://localhost/resque",
			expectedError: errorInvalidScheme,
		},
		{
			uri: "redis+sentinel://localhost",
			expectedError: errorMasterNameMissing,
		},
		{
			uri: "redis+sentinel://localhost/resque",
			expectedError: nil,
		},
		{
			uri: "redis+sentinel://localhost:26379,otherhost:26379/resque/5",
			expectedError: nil,
		},
	} {
		_, err := newRedisPool(tt.uri, 1, 1, time.Minute)
		if tt.expectedError == nil && err != nil {
			t.Error(tt.uri, "did not expect an error, got", err)
		} else if tt.expectedError != err {
			t.Error(tt.uri, "expected", tt.expectedError, "got", err)
		}
	}
}

func TestEnqueue(t *testing.T) {
	dockerComposeUp(t)
	setup := scanSetup(t)
	ensureMaster(setup, t)
	dockerClearDb(setup, t)
	defer func() {
		dockerComposeStop(t)
	}()

	expectedArgs := []interface{}{"a", "lot", "of", "params"}
	jobName := "SomethingCool"
	queueName := "testQueue"
	expectedJob := &Job{
		Queue: queueName,
		Payload: Payload{
			Class: jobName,
			Args:  expectedArgs,
		},
	}

	workerSettings.URI = fmt.Sprintf(
		"redis+sentinel://%s:26379/resque",
		setup["sentinel"][0].address,
	)
	workerSettings.Queues = []string{queueName}
	workerSettings.UseNumber = true
	workerSettings.ExitOnComplete = true
	workerSettings.Timeout = time.Minute

	err := Enqueue(expectedJob)
	if err != nil {
		t.Errorf("Error while enqueue %s", err)
	}

	actualArgs := []interface{}{}
	actualQueueName := ""
	Register(jobName, func(queue string, args ...interface{}) error {
		actualArgs = args
		actualQueueName = queue
		return nil
	})
	if err := Work(); err != nil {
		t.Errorf("(Enqueue) Failed on work %s", err)
	}
	if !reflect.DeepEqual(actualArgs, expectedArgs) {
		t.Errorf("(Enqueue) Expected %v, actual %v", actualArgs, expectedArgs)
	}
	if !reflect.DeepEqual(actualQueueName, queueName) {
		t.Errorf("(Enqueue) Expected %v, actual %v", actualQueueName, queueName)
	}
}
