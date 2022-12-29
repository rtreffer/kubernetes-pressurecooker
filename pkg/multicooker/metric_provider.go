package multicooker

import (
	"fmt"

	"github.com/prometheus/procfs"
)

func (w *Watcher) CollectMetricsData(closeChan chan struct{}, useAvarage bool, metricChan chan [3]float64, errorChan chan error) {

	if useAvarage {
		avg, err := w.proc.LoadAvg()
		if err != nil {
			errorChan <- err
			return
		}

		if avg == nil {
			errorChan <- fmt.Errorf("could not load cpu avarage, got %v", avg)
			return
		}

		metricChan <- extactWorkingData(nil, avg, useAvarage)

	} else {
		cpu, err := w.proc.PSIStatsForResource("cpu")
		if err != nil {
			errorChan <- err
			return
		}

		if cpu.Some == nil {
			errorChan <- fmt.Errorf("could not load cpu pressure, got %v", cpu)
			return
		}

		metricChan <- extactWorkingData(cpu.Some, nil, useAvarage)
	}
}

func extactWorkingData(cpu *procfs.PSILine, ava *procfs.LoadAvg, useAvarage bool) [3]float64 {
	if useAvarage {
		return [3]float64{ava.Load1, ava.Load5, ava.Load15}
	} else {
		return [3]float64{cpu.Avg10, cpu.Avg60, cpu.Avg300}
	}
}
