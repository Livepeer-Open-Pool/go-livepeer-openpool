package events

// ** Pool Customization **
import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/glog"
	"github.com/google/uuid"
)

// EventTracker defines the methods a tracker must implement.
type EventTracker interface {
	CreateEventLog(eventType string, pairs ...interface{})
	Stop()
}

// GlobalEventTracker is accessible from anywhere that imports this package.
var GlobalEventTracker EventTracker

// NoopEventTracker is a no-operation tracker that prints events to the console.
type NoopEventTracker struct{}

// PoolTrackerOptions holds all of InitPoolTracker’s inputs.
type PoolTrackerOptions struct {
	UseNoop       bool
	Threshold     int           // e.g. POOL_EVENT_THRESHOLD (default 10000)
	FlushInterval time.Duration // e.g. POOL_FLUSH_INTERVAL_SECONDS (default 60s)

	S3Host      string // POOL_S3_HOST
	S3Bucket    string // POOL_S3_BUCKET
	S3AccessKey string // POOL_S3_ACCESS_KEY
	S3SecretKey string // POOL_S3_SECRET_KEY
	Region      string // POOL_REGION
	NodeType    string // POOL_NODE_TYPE
}

// CreateEventLog for the noop tracker simply logs the event details.
func (n *NoopEventTracker) CreateEventLog(eventType string, pairs ...interface{}) {
	glog.Infof("NoopEventTracker: event %s, pairs: %v", eventType, pairs)
}

// Stop for the noop tracker logs that Stop was called.
func (n *NoopEventTracker) Stop() {
	glog.Infof("NoopEventTracker: Stop called")
}

// eventLog represents an event to be logged and now includes a timestamp.
type eventLog struct {
	eventType string
	pairs     []interface{}
	timestamp time.Time
}

// eventTracker tracks events in memory until a threshold is reached.
// When the threshold is met, events are dumped to an S3 bucket.
type eventTracker struct {
	events        chan eventLog
	quit          chan struct{}
	threshold     int
	flushInterval time.Duration
	accumulated   []eventLog
	s3Bucket      string
	region        string
	nodeType      string
	s3Client      *s3.S3
	mu            sync.Mutex
}

// NewEventTracker creates a new tracker using S3 for dumping events.
func NewEventTracker(threshold int, flushInterval time.Duration, s3host, s3Bucket, s3AccessKey, s3SecretKey, region, nodeType string) *eventTracker {
	glog.Infof("eventTracker: creating new instance with S3 integration")
	// Create an AWS session (ensure your AWS credentials are properly configured).
	sess, err := session.NewSession(&aws.Config{
		S3ForcePathStyle: aws.Bool(true),
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String(s3host),
		Credentials:      credentials.NewStaticCredentials(s3AccessKey, s3SecretKey, ""),
	})
	if err != nil {
		glog.Fatalf("Failed to create S3 session: %v", err)
	}
	s3Client := s3.New(sess)

	return &eventTracker{
		events:        make(chan eventLog, threshold),
		quit:          make(chan struct{}),
		threshold:     threshold,
		accumulated:   make([]eventLog, 0, threshold),
		flushInterval: flushInterval,
		s3Bucket:      s3Bucket,
		region:        region,
		nodeType:      nodeType,
		s3Client:      s3Client,
	}
}

// startEventProcessor processes incoming events, accumulates them in memory,
// and flushes to S3 once the threshold is reached.
func (p *eventTracker) startEventProcessor() {
	ticker := time.NewTicker(p.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case e, ok := <-p.events:
			if !ok {
				// Channel closed; flush remaining events.
				p.mu.Lock()
				hasEvents := len(p.accumulated) > 0
				p.mu.Unlock()
				if hasEvents {
					p.flushEvents()
				}
				return
			}
			p.mu.Lock()
			p.accumulated = append(p.accumulated, e)
			shouldFlush := len(p.accumulated) >= p.threshold
			p.mu.Unlock()
			if shouldFlush {
				p.flushEvents()
			}
		case <-ticker.C:
			// Flush based on time interval.
			p.mu.Lock()
			hasEvents := len(p.accumulated) > 0
			p.mu.Unlock()
			if hasEvents {
				p.flushEvents()
			}
		case <-p.quit:
			// Flush any remaining events upon shutdown.
			ticker.Stop()
			p.mu.Lock()
			hasEvents := len(p.accumulated) > 0
			p.mu.Unlock()
			if hasEvents {
				p.flushEvents()
			}
			return
		}
	}
}

// flushEvents converts the accumulated events to JSON and uploads them to S3.
func (p *eventTracker) flushEvents() {
	// Safely capture and clear the accumulated events.
	p.mu.Lock()
	if len(p.accumulated) == 0 {
		p.mu.Unlock()
		return
	}
	eventsToFlush := make([]eventLog, len(p.accumulated))
	copy(eventsToFlush, p.accumulated)
	p.accumulated = p.accumulated[:0]
	p.mu.Unlock()

	// Determine start and end event timestamps.
	startTime := eventsToFlush[0].timestamp
	endTime := eventsToFlush[len(eventsToFlush)-1].timestamp

	// Build an array of JSON event objects.
	var eventsArray []map[string]interface{}
	for _, e := range eventsToFlush {
		// Ensure an even number of key-value pairs.
		if len(e.pairs)%2 != 0 {
			glog.Errorf("eventTracker: uneven number of arguments for event %s; expected key-value pairs", e.eventType)
			continue
		}
		payload := make(map[string]interface{})
		for i := 0; i < len(e.pairs); i += 2 {
			key, ok := e.pairs[i].(string)
			if !ok {
				glog.Errorf("eventTracker: key must be a string, got %T for event %s", e.pairs[i], e.eventType)
				continue
			}
			payload[key] = e.pairs[i+1]
		}
		outerEvent := map[string]interface{}{
			"ID":        uuid.New().String(),
			"EventType": e.eventType,
			"Version":   1,
			"Payload":   payload,
			"NodeType":  p.nodeType,
			"Region":    p.region,
			"DT":        e.timestamp.Format(time.RFC3339),
		}
		eventsArray = append(eventsArray, outerEvent)
	}

	// Marshal the events array to indented JSON.
	jsonData, err := json.MarshalIndent(eventsArray, "", "  ")
	if err != nil {
		glog.Errorf("eventTracker: error marshalling events: %v", err)
		return
	}

	// Construct a file name including region, node type, and the start/end event timestamps.
	fileName := fmt.Sprintf("%s-%s-%s_%s.json",
		p.region,
		p.nodeType,
		startTime.Format("20060102T150405Z"),
		endTime.Format("20060102T150405Z"))

	// Upload the JSON data to S3.
	_, err = p.s3Client.PutObject(&s3.PutObjectInput{
		Bucket:      aws.String(p.s3Bucket),
		Key:         aws.String(fmt.Sprintf("%s/%s/%s", p.region, p.nodeType, fileName)),
		Body:        bytes.NewReader(jsonData),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		glog.Errorf("eventTracker: error uploading events to S3: %v", err)
	} else {
		glog.Infof("eventTracker: Successfully uploaded %d events to S3 as %s", len(eventsToFlush), fileName)
	}
}

// CreateEventLog enqueues an event for asynchronous processing.
func (p *eventTracker) CreateEventLog(eventType string, pairs ...interface{}) {
	glog.Infof("eventTracker: Enqueuing event: %s, pairs: %v", eventType, pairs)
	e := eventLog{
		eventType: eventType,
		pairs:     pairs,
		timestamp: time.Now().UTC(),
	}
	p.events <- e
}

// Stop gracefully shuts down the event processor and flushes any remaining events.
func (p *eventTracker) Stop() {
	close(p.quit)
	close(p.events)
	p.mu.Lock()
	hasEvents := len(p.accumulated) > 0
	p.mu.Unlock()
	if hasEvents {
		p.flushEvents()
	}
}

// InitPoolTracker initializes the global tracker.
// If useNoop is true, it returns a Noop version that prints events to the console.
// Otherwise, it creates a eventTracker that flushes events to S3.
func InitPoolTracker(opts PoolTrackerOptions) {
	if opts.UseNoop {
		glog.Infof("Initializing NoopEventTracker")
		GlobalEventTracker = &NoopEventTracker{}
		return
	}

	realTracker := NewEventTracker(
		opts.Threshold,
		opts.FlushInterval,
		opts.S3Host,
		opts.S3Bucket,
		opts.S3AccessKey,
		opts.S3SecretKey,
		opts.Region,
		opts.NodeType,
	)
	GlobalEventTracker = realTracker
	go realTracker.startEventProcessor()
}
