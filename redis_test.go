package goworker

import (
	"testing"
	"time"
)

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
		_, err := newSentinel(tt.uri, time.Minute)
		if tt.expectedError == nil && err != nil {
			t.Error(tt.uri, "did not expect an error, got", err)
		} else if tt.expectedError != err {
			t.Error(tt.uri, "expected", tt.expectedError, "got", err)
		}
	}
}

