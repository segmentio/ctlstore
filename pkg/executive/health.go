package executive

type HealthChecker interface {
	HealthCheck() error
}
