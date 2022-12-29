package multicooker

import (
	"time"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
)

type Evicter struct {
	client       kubernetes.Interface
	nodeName     string
	nodeRef      *v1.ObjectReference
	recorder     record.EventRecorder
	minPodAge    time.Duration
	backoff      time.Duration
	lastEviction time.Time
}

func NewEvicter(client kubernetes.Interface, nodeName string, backoff string, minPodAge string) (*Evicter, error) {

	backoffDuration, err := time.ParseDuration(backoff)
	if err != nil {
		return nil, err
	}

	minPodAgeDuration, err := time.ParseDuration(minPodAge)
	if err != nil {
		return nil, err
	}

	b := record.NewBroadcaster()
	b.StartLogging(glog.Infof)
	b.StartRecordingToSink(&typedv1.EventSinkImpl{
		Interface: client.CoreV1().Events(""),
	})

	r := b.NewRecorder(scheme.Scheme, v1.EventSource{Host: nodeName, Component: ComponentName + "/evicter"})

	nodeRef := &v1.ObjectReference{
		Kind:      "Node",
		Name:      nodeName,
		UID:       types.UID(nodeName),
		Namespace: "",
	}

	return &Evicter{
		client:    client,
		nodeName:  nodeName,
		nodeRef:   nodeRef,
		recorder:  r,
		backoff:   backoffDuration,
		minPodAge: minPodAgeDuration,
	}, nil
}
