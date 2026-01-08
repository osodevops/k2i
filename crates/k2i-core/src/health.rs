//! Health check system for monitoring component status.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::Instant;

/// Health status of a component.
#[derive(Debug, Clone, PartialEq)]
pub enum ComponentStatus {
    /// Component is healthy
    Healthy,
    /// Component is degraded but operational
    Degraded(String),
    /// Component is unhealthy
    Unhealthy(String),
    /// Component status is unknown
    Unknown,
}

/// Overall system health status.
#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    /// All components healthy
    Healthy,
    /// Some components degraded
    Degraded,
    /// System is unhealthy
    Unhealthy,
}

/// Health check manager for tracking component health.
pub struct HealthCheck {
    components: RwLock<HashMap<String, ComponentStatus>>,
    started_at: Option<Instant>,
    job_running: RwLock<bool>,
}

impl HealthCheck {
    /// Create a new health check manager.
    pub fn new() -> Self {
        Self {
            components: RwLock::new(HashMap::new()),
            started_at: None,
            job_running: RwLock::new(false),
        }
    }

    /// Register a component to track.
    pub fn register_component(&self, name: &str) {
        let mut components = self.components.write();
        components.insert(name.to_string(), ComponentStatus::Unknown);
    }

    /// Mark a component as healthy.
    pub fn mark_healthy(&self, name: &str) {
        let mut components = self.components.write();
        components.insert(name.to_string(), ComponentStatus::Healthy);
    }

    /// Mark a component as degraded.
    pub fn mark_degraded(&self, name: &str, reason: &str) {
        let mut components = self.components.write();
        components.insert(
            name.to_string(),
            ComponentStatus::Degraded(reason.to_string()),
        );
    }

    /// Mark a component as unhealthy.
    pub fn mark_unhealthy(&self, name: &str, reason: &str) {
        let mut components = self.components.write();
        components.insert(
            name.to_string(),
            ComponentStatus::Unhealthy(reason.to_string()),
        );
    }

    /// Get the status of a specific component.
    pub fn get_component_status(&self, name: &str) -> Option<ComponentStatus> {
        let components = self.components.read();
        components.get(name).cloned()
    }

    /// Get all component statuses.
    pub fn get_all_statuses(&self) -> HashMap<String, ComponentStatus> {
        self.components.read().clone()
    }

    /// Get overall system health status.
    pub fn overall_status(&self) -> HealthStatus {
        let components = self.components.read();

        let mut has_degraded = false;
        for status in components.values() {
            match status {
                ComponentStatus::Unhealthy(_) => return HealthStatus::Unhealthy,
                ComponentStatus::Degraded(_) => has_degraded = true,
                _ => {}
            }
        }

        if has_degraded {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        }
    }

    /// Check if the system is operational (healthy or degraded).
    pub fn is_operational(&self) -> bool {
        matches!(
            self.overall_status(),
            HealthStatus::Healthy | HealthStatus::Degraded
        )
    }

    /// Mark the ingestion job as started.
    pub fn job_started(&self) {
        *self.job_running.write() = true;
    }

    /// Mark the ingestion job as completed.
    pub fn job_completed(&self) {
        *self.job_running.write() = false;
    }

    /// Check if the ingestion job is running.
    pub fn is_job_running(&self) -> bool {
        *self.job_running.read()
    }
}

impl Default for HealthCheck {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_check_lifecycle() {
        let health = HealthCheck::new();

        health.register_component("kafka");
        health.register_component("iceberg");

        assert_eq!(
            health.get_component_status("kafka"),
            Some(ComponentStatus::Unknown)
        );

        health.mark_healthy("kafka");
        assert_eq!(
            health.get_component_status("kafka"),
            Some(ComponentStatus::Healthy)
        );

        health.mark_healthy("iceberg");
        assert_eq!(health.overall_status(), HealthStatus::Healthy);

        health.mark_degraded("kafka", "high latency");
        assert_eq!(health.overall_status(), HealthStatus::Degraded);
        assert!(health.is_operational());

        health.mark_unhealthy("kafka", "connection lost");
        assert_eq!(health.overall_status(), HealthStatus::Unhealthy);
        assert!(!health.is_operational());
    }

    #[test]
    fn test_health_check_default() {
        let health = HealthCheck::default();
        assert_eq!(health.overall_status(), HealthStatus::Healthy);
        assert!(health.is_operational());
    }

    #[test]
    fn test_health_check_unknown_component() {
        let health = HealthCheck::new();
        assert_eq!(health.get_component_status("unknown"), None);
    }

    #[test]
    fn test_health_check_get_all_statuses() {
        let health = HealthCheck::new();
        health.register_component("kafka");
        health.register_component("iceberg");
        health.mark_healthy("kafka");
        health.mark_degraded("iceberg", "slow");

        let statuses = health.get_all_statuses();
        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses.get("kafka"), Some(&ComponentStatus::Healthy));
        assert!(matches!(
            statuses.get("iceberg"),
            Some(ComponentStatus::Degraded(_))
        ));
    }

    #[test]
    fn test_health_check_job_lifecycle() {
        let health = HealthCheck::new();

        assert!(!health.is_job_running());

        health.job_started();
        assert!(health.is_job_running());

        health.job_completed();
        assert!(!health.is_job_running());
    }

    #[test]
    fn test_health_status_priority() {
        let health = HealthCheck::new();
        health.register_component("a");
        health.register_component("b");
        health.register_component("c");

        // All healthy
        health.mark_healthy("a");
        health.mark_healthy("b");
        health.mark_healthy("c");
        assert_eq!(health.overall_status(), HealthStatus::Healthy);

        // One degraded
        health.mark_degraded("b", "slow");
        assert_eq!(health.overall_status(), HealthStatus::Degraded);

        // One unhealthy (should override degraded)
        health.mark_unhealthy("c", "down");
        assert_eq!(health.overall_status(), HealthStatus::Unhealthy);
    }

    #[test]
    fn test_component_status_transitions() {
        let health = HealthCheck::new();
        health.register_component("test");

        // Unknown -> Healthy
        health.mark_healthy("test");
        assert_eq!(
            health.get_component_status("test"),
            Some(ComponentStatus::Healthy)
        );

        // Healthy -> Degraded
        health.mark_degraded("test", "warning");
        assert_eq!(
            health.get_component_status("test"),
            Some(ComponentStatus::Degraded("warning".to_string()))
        );

        // Degraded -> Unhealthy
        health.mark_unhealthy("test", "error");
        assert_eq!(
            health.get_component_status("test"),
            Some(ComponentStatus::Unhealthy("error".to_string()))
        );

        // Unhealthy -> Healthy (recovery)
        health.mark_healthy("test");
        assert_eq!(
            health.get_component_status("test"),
            Some(ComponentStatus::Healthy)
        );
    }
}
