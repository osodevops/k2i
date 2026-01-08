//! Circuit breaker pattern for fault tolerance.
//!
//! Prevents cascading failures by temporarily blocking calls to failing services.

use parking_lot::Mutex;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CircuitState {
    /// Circuit is closed, operations proceed normally
    Closed,
    /// Circuit is open, operations are blocked
    Open,
    /// Circuit is half-open, testing with single request
    HalfOpen,
}

/// Circuit breaker configuration.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening the circuit
    pub failure_threshold: u32,

    /// Duration to wait before attempting to close (half-open)
    pub reset_timeout: Duration,

    /// Number of successes required to close from half-open
    pub success_threshold: u32,

    /// Name for logging
    pub name: String,
}

impl CircuitBreakerConfig {
    /// Create a new circuit breaker configuration.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            failure_threshold: 5,
            reset_timeout: Duration::from_secs(30),
            success_threshold: 2,
            name: name.into(),
        }
    }

    /// Set failure threshold.
    pub fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold;
        self
    }

    /// Set reset timeout.
    pub fn with_reset_timeout(mut self, timeout: Duration) -> Self {
        self.reset_timeout = timeout;
        self
    }

    /// Set success threshold.
    pub fn with_success_threshold(mut self, threshold: u32) -> Self {
        self.success_threshold = threshold;
        self
    }
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self::new("default")
    }
}

/// Internal state tracking.
struct CircuitBreakerState {
    state: CircuitState,
    failure_count: u32,
    success_count: u32,
    last_failure_time: Option<Instant>,
    last_state_change: Instant,
}

impl CircuitBreakerState {
    fn new() -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            success_count: 0,
            last_failure_time: None,
            last_state_change: Instant::now(),
        }
    }
}

/// Circuit breaker for external service calls.
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Mutex<CircuitBreakerState>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        info!(name = %config.name, "Circuit breaker created");
        Self {
            config,
            state: Mutex::new(CircuitBreakerState::new()),
        }
    }

    /// Check if the circuit allows requests.
    pub fn is_allowed(&self) -> bool {
        let mut state = self.state.lock();

        match state.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if we should transition to half-open
                if let Some(last_failure) = state.last_failure_time {
                    if last_failure.elapsed() >= self.config.reset_timeout {
                        state.state = CircuitState::HalfOpen;
                        state.success_count = 0;
                        state.last_state_change = Instant::now();
                        info!(
                            name = %self.config.name,
                            "Circuit breaker transitioning to half-open"
                        );
                        return true;
                    }
                }
                false
            }
            CircuitState::HalfOpen => true,
        }
    }

    /// Record a successful operation.
    pub fn record_success(&self) {
        let mut state = self.state.lock();

        match state.state {
            CircuitState::Closed => {
                // Reset failure count on success
                state.failure_count = 0;
            }
            CircuitState::HalfOpen => {
                state.success_count += 1;
                debug!(
                    name = %self.config.name,
                    success_count = state.success_count,
                    threshold = self.config.success_threshold,
                    "Circuit breaker recorded success in half-open state"
                );

                if state.success_count >= self.config.success_threshold {
                    state.state = CircuitState::Closed;
                    state.failure_count = 0;
                    state.success_count = 0;
                    state.last_state_change = Instant::now();
                    info!(
                        name = %self.config.name,
                        "Circuit breaker closed (recovered)"
                    );
                }
            }
            CircuitState::Open => {
                // Shouldn't happen, but reset if it does
                state.failure_count = 0;
            }
        }
    }

    /// Record a failed operation.
    pub fn record_failure(&self) {
        let mut state = self.state.lock();
        state.failure_count += 1;
        state.last_failure_time = Some(Instant::now());

        match state.state {
            CircuitState::Closed => {
                if state.failure_count >= self.config.failure_threshold {
                    state.state = CircuitState::Open;
                    state.last_state_change = Instant::now();
                    warn!(
                        name = %self.config.name,
                        failures = state.failure_count,
                        "Circuit breaker opened due to failures"
                    );
                } else {
                    debug!(
                        name = %self.config.name,
                        failures = state.failure_count,
                        threshold = self.config.failure_threshold,
                        "Circuit breaker recorded failure"
                    );
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in half-open immediately opens the circuit
                state.state = CircuitState::Open;
                state.success_count = 0;
                state.last_state_change = Instant::now();
                warn!(
                    name = %self.config.name,
                    "Circuit breaker reopened from half-open state"
                );
            }
            CircuitState::Open => {
                // Already open, just record the failure
                debug!(
                    name = %self.config.name,
                    "Failure recorded while circuit is open"
                );
            }
        }
    }

    /// Get the current state.
    pub fn state(&self) -> CircuitState {
        self.state.lock().state
    }

    /// Get the failure count.
    pub fn failure_count(&self) -> u32 {
        self.state.lock().failure_count
    }

    /// Get the name.
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Reset the circuit breaker to closed state.
    pub fn reset(&self) {
        let mut state = self.state.lock();
        state.state = CircuitState::Closed;
        state.failure_count = 0;
        state.success_count = 0;
        state.last_failure_time = None;
        state.last_state_change = Instant::now();
        info!(name = %self.config.name, "Circuit breaker reset");
    }

    /// Execute a function with circuit breaker protection.
    pub async fn execute<F, Fut, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        if !self.is_allowed() {
            return Err(CircuitBreakerError::CircuitOpen);
        }

        match f().await {
            Ok(result) => {
                self.record_success();
                Ok(result)
            }
            Err(e) => {
                self.record_failure();
                Err(CircuitBreakerError::ServiceError(e))
            }
        }
    }

    /// Execute a synchronous function with circuit breaker protection.
    pub fn execute_sync<F, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Result<T, E>,
    {
        if !self.is_allowed() {
            return Err(CircuitBreakerError::CircuitOpen);
        }

        match f() {
            Ok(result) => {
                self.record_success();
                Ok(result)
            }
            Err(e) => {
                self.record_failure();
                Err(CircuitBreakerError::ServiceError(e))
            }
        }
    }
}

/// Circuit breaker error.
#[derive(Debug)]
pub enum CircuitBreakerError<E> {
    /// Circuit is open, call was not attempted
    CircuitOpen,
    /// Service returned an error
    ServiceError(E),
}

impl<E: std::fmt::Display> std::fmt::Display for CircuitBreakerError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitBreakerError::CircuitOpen => write!(f, "Circuit breaker is open"),
            CircuitBreakerError::ServiceError(e) => write!(f, "Service error: {}", e),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for CircuitBreakerError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CircuitBreakerError::CircuitOpen => None,
            CircuitBreakerError::ServiceError(e) => Some(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::new("test"));
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.is_allowed());
    }

    #[test]
    fn test_circuit_breaker_opens_after_failures() {
        let config = CircuitBreakerConfig::new("test").with_failure_threshold(3);
        let cb = CircuitBreaker::new(config);

        // First two failures don't open the circuit
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.is_allowed());

        // Third failure opens the circuit
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.is_allowed());
    }

    #[test]
    fn test_circuit_breaker_success_resets_failures() {
        let config = CircuitBreakerConfig::new("test").with_failure_threshold(3);
        let cb = CircuitBreaker::new(config);

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.failure_count(), 2);

        cb.record_success();
        assert_eq!(cb.failure_count(), 0);
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_transitions_to_half_open() {
        let config = CircuitBreakerConfig::new("test")
            .with_failure_threshold(2)
            .with_reset_timeout(Duration::from_millis(10));
        let cb = CircuitBreaker::new(config);

        // Open the circuit
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.is_allowed());

        // Wait for reset timeout
        std::thread::sleep(Duration::from_millis(15));

        // Should transition to half-open on next check
        assert!(cb.is_allowed());
        assert_eq!(cb.state(), CircuitState::HalfOpen);
    }

    #[test]
    fn test_circuit_breaker_closes_from_half_open() {
        let config = CircuitBreakerConfig::new("test")
            .with_failure_threshold(2)
            .with_reset_timeout(Duration::from_millis(10))
            .with_success_threshold(2);
        let cb = CircuitBreaker::new(config);

        // Open the circuit
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        // Wait and transition to half-open
        std::thread::sleep(Duration::from_millis(15));
        cb.is_allowed();
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        // Success in half-open
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        // Second success closes the circuit
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_reopens_from_half_open_on_failure() {
        let config = CircuitBreakerConfig::new("test")
            .with_failure_threshold(2)
            .with_reset_timeout(Duration::from_millis(10));
        let cb = CircuitBreaker::new(config);

        // Open the circuit
        cb.record_failure();
        cb.record_failure();

        // Wait and transition to half-open
        std::thread::sleep(Duration::from_millis(15));
        cb.is_allowed();
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        // Failure in half-open reopens
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_circuit_breaker_reset() {
        let config = CircuitBreakerConfig::new("test").with_failure_threshold(2);
        let cb = CircuitBreaker::new(config);

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        cb.reset();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert_eq!(cb.failure_count(), 0);
        assert!(cb.is_allowed());
    }

    #[test]
    fn test_circuit_breaker_execute_sync() {
        let config = CircuitBreakerConfig::new("test").with_failure_threshold(2);
        let cb = CircuitBreaker::new(config);

        // Successful execution
        let result: Result<i32, CircuitBreakerError<&str>> = cb.execute_sync(|| Ok::<_, &str>(42));
        assert!(matches!(result, Ok(42)));

        // Failed execution
        let result: Result<i32, CircuitBreakerError<&str>> =
            cb.execute_sync(|| Err::<i32, _>("error"));
        assert!(matches!(result, Err(CircuitBreakerError::ServiceError(_))));

        // Another failure opens circuit
        let _ = cb.execute_sync(|| Err::<i32, &str>("error"));
        assert_eq!(cb.state(), CircuitState::Open);

        // Circuit open prevents execution
        let result: Result<i32, CircuitBreakerError<&str>> = cb.execute_sync(|| Ok::<_, &str>(42));
        assert!(matches!(result, Err(CircuitBreakerError::CircuitOpen)));
    }
}
