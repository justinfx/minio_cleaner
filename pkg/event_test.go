package pkg

import (
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/notification"
	"github.com/stretchr/testify/require"
)

func TestBucketEvent_Type(t *testing.T) {
	tests := []struct {
		eventType notification.EventType
		want      BucketEventType
	}{
		{notification.ObjectAccessedGet, BucketEventRead},
		{notification.ObjectCreatedCopy, BucketEventWrite},
		{notification.ObjectCreatedPut, BucketEventWrite},
		{notification.ObjectCreatedPost, BucketEventWrite},
		{notification.ObjectRemovedDelete, BucketEventDelete},
		{notification.ObjectCreatedPutLegalHold, BucketEventOther},
	}
	for _, tt := range tests {
		e := &BucketEvent{EventName: tt.eventType}
		if got := e.Type(); got != tt.want {
			t.Errorf("For event %v, go = %v, want %v", e.EventName, got, tt.want)
		}
	}
}

func TestBucketEvent_IsStat(t *testing.T) {
	tests := []struct {
		eventType notification.EventType
		want      bool
	}{
		{notification.ObjectAccessedGet, false},
		{notification.ObjectAccessedHead, true},
	}
	for _, tt := range tests {
		e := &BucketEvent{EventName: tt.eventType}
		if got := e.IsStat(); got != tt.want {
			t.Errorf("For event %v, got = %v, want %v", e.EventName, got, tt.want)
		}
	}
}

func TestBucketEvent_Check(t *testing.T) {
	evt := &BucketEvent{}
	require.Error(t, evt.Check())

	evt = newBucketEvent(notification.ObjectCreatedCopy, "", "")
	require.Error(t, evt.Check())

	evt.Records[0].S3.Bucket.Name = "bucket"
	require.Error(t, evt.Check())

	evt.Records[0].S3.Object.Key = "key"
	require.NoError(t, evt.Check())
}

func TestBucketEvent_SetEventTime(t *testing.T) {
	evt := &BucketEvent{}
	evt.SetEventTime(time.Now())
	require.Len(t, evt.Records, 0)

	now := time.Now()
	later := now.Add(time.Hour)

	evt.Records = append(evt.Records, record{EventTime: now})
	require.Len(t, evt.Records, 1)

	evt.SetEventTime(later)
	require.Equal(t, evt.Records[0].EventTime, later)

	evt.SetEventTime(now)
	require.Equal(t, evt.Records[0].EventTime, now)
}
