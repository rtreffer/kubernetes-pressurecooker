package multicooker

import (
	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
)

const ComponentName = "multicooker"

const TaintKey = "multicooker/load-exceeded"

type Tainter struct {
	client   kubernetes.Interface
	recorder record.EventRecorder
	nodeName string
	nodeRef  *v1.ObjectReference
}

func NewTainter(c kubernetes.Interface, nodeName string) (*Tainter, error) {
	b := record.NewBroadcaster()
	b.StartLogging(glog.Infof)
	b.StartRecordingToSink(&typedv1.EventSinkImpl{
		Interface: c.CoreV1().Events(""),
	})

	r := b.NewRecorder(scheme.Scheme, v1.EventSource{Host: nodeName, Component: ComponentName + "/tainter"})

	nodeRef := &v1.ObjectReference{
		Kind: "Node",
		Name: nodeName,
		UID:  types.UID(nodeName),
	}

	return &Tainter{
		client:   c,
		recorder: r,
		nodeName: nodeName,
		nodeRef:  nodeRef,
	}, nil
}
