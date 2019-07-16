package ledger

import "time"

func WithCheckCallback(fn func()) MonitorOpt {
	return func(m *Monitor) {
		m.checkCallback = fn
	}
}

func WithECSClient(ecsClient ECSClient) MonitorOpt {
	return func(m *Monitor) {
		m.ecsClient = ecsClient
	}
}

func WithECSMetadataFunc(fn ecsMetadataFunc) MonitorOpt {
	return func(m *Monitor) {
		m.ecsMetadataFunc = fn
	}
}

func WithTicker(ticker *time.Ticker) MonitorOpt {
	return func(m *Monitor) {
		m.tickerFunc = func() *time.Ticker {
			return ticker
		}
	}
}
