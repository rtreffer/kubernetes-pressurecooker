package pressurecooker

import (
	"time"

	"github.com/prometheus/procfs"
)

type PressureThresholdEvent procfs.PSILine

type Watcher struct {
	TickerInterval    time.Duration
	PressureThreshold float64

	proc            procfs.FS
	isCurrentlyHigh bool
}

func NewWatcher(pressureThreshold float64) (*Watcher, error) {
	if pressureThreshold == 0 {
		pressureThreshold = 25
	}

	fs, err := procfs.NewDefaultFS()
	if err != nil {
		return nil, err
	}

	return &Watcher{
		PressureThreshold: pressureThreshold,
		TickerInterval:    15 * time.Second,
		proc:              fs,
	}, nil
}
