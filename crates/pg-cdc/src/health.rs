use std::fmt;
use std::sync::Arc;

use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Role {
    #[default]
    Standby,
    Active,
    Draining,
}

impl fmt::Display for Role {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Standby => "standby",
            Self::Active => "active",
            Self::Draining => "draining",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent probe flags that /healthz and /readyz combine"
)]
pub struct HealthSnapshot {
    pub sink_ready: bool,
    pub store_ready: bool,
    pub validated: bool,
    pub role: Role,
    pub lease_valid: bool,
    pub delivering: bool,
}

impl HealthSnapshot {
    pub fn healthy(&self) -> bool {
        self.sink_ready
            && self.store_ready
            && self.validated
            && (self.role == Role::Standby || self.lease_valid)
    }

    pub fn ready(&self) -> bool {
        self.healthy() && self.role == Role::Active && self.delivering
    }
}

#[derive(Debug, Clone)]
pub struct Health {
    state: Arc<watch::Sender<HealthSnapshot>>,
}

impl Default for Health {
    fn default() -> Self {
        Self::new()
    }
}

impl Health {
    pub fn new() -> Self {
        Self {
            state: Arc::new(watch::Sender::new(HealthSnapshot::default())),
        }
    }

    pub fn snapshot(&self) -> HealthSnapshot {
        *self.state.borrow()
    }

    pub fn subscribe(&self) -> watch::Receiver<HealthSnapshot> {
        self.state.subscribe()
    }

    pub fn set_sink_ready(&self, ready: bool) {
        self.set(|snapshot| &mut snapshot.sink_ready, ready);
    }

    pub fn set_store_ready(&self, ready: bool) {
        self.set(|snapshot| &mut snapshot.store_ready, ready);
    }

    pub fn set_validated(&self, validated: bool) {
        self.set(|snapshot| &mut snapshot.validated, validated);
    }

    pub fn set_role(&self, role: Role) {
        self.state.send_if_modified(|snapshot| {
            let from = snapshot.role;
            if from == role {
                return false;
            }
            snapshot.role = role;
            tracing::info!(%from, to = %role, "role changed");
            true
        });
    }

    pub fn set_lease_valid(&self, valid: bool) {
        self.set(|snapshot| &mut snapshot.lease_valid, valid);
    }

    pub fn set_delivering(&self, delivering: bool) {
        self.set(|snapshot| &mut snapshot.delivering, delivering);
    }

    fn set(&self, field: impl FnOnce(&mut HealthSnapshot) -> &mut bool, value: bool) {
        self.state.send_if_modified(|snapshot| {
            let slot = field(snapshot);
            let modified = *slot != value;
            *slot = value;
            modified
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checked() -> Health {
        let health = Health::new();
        health.set_sink_ready(true);
        health.set_store_ready(true);
        health.set_validated(true);
        health
    }

    #[test]
    fn a_new_task_is_unhealthy_until_every_check_passes() {
        let health = Health::new();
        assert!(!health.snapshot().healthy());
        health.set_sink_ready(true);
        assert!(!health.snapshot().healthy());
        health.set_store_ready(true);
        assert!(!health.snapshot().healthy());
        health.set_validated(true);
        let snapshot = health.snapshot();
        assert_eq!(snapshot.role, Role::Standby);
        assert!(snapshot.healthy());
        assert!(!snapshot.ready());
    }

    #[test]
    fn an_active_task_is_healthy_only_while_its_lease_is_valid() {
        let health = checked();
        for role in [Role::Active, Role::Draining] {
            health.set_role(role);
            health.set_lease_valid(false);
            assert!(!health.snapshot().healthy(), "{role}");
            health.set_lease_valid(true);
            assert!(health.snapshot().healthy(), "{role}");
        }
    }

    #[test]
    fn a_failed_check_makes_a_task_unhealthy_in_any_role() {
        for role in [Role::Standby, Role::Active, Role::Draining] {
            for failing in 0..3 {
                let health = checked();
                health.set_role(role);
                health.set_lease_valid(true);
                match failing {
                    0 => health.set_sink_ready(false),
                    1 => health.set_store_ready(false),
                    _ => health.set_validated(false),
                }
                assert!(!health.snapshot().healthy(), "{role} {failing}");
            }
        }
    }

    #[test]
    fn ready_needs_health_an_active_role_and_delivery() {
        let health = checked();
        health.set_delivering(true);
        assert!(!health.snapshot().ready());

        health.set_role(Role::Active);
        health.set_lease_valid(true);
        assert!(health.snapshot().ready());

        health.set_delivering(false);
        assert!(!health.snapshot().ready());
        health.set_delivering(true);

        health.set_lease_valid(false);
        assert!(!health.snapshot().ready());
        health.set_lease_valid(true);

        health.set_role(Role::Draining);
        assert!(!health.snapshot().ready());
        health.set_role(Role::Active);

        health.set_validated(false);
        assert!(!health.snapshot().ready());
    }

    #[test]
    fn roles_display_as_lowercase_words() {
        assert_eq!(Role::Standby.to_string(), "standby");
        assert_eq!(Role::Active.to_string(), "active");
        assert_eq!(Role::Draining.to_string(), "draining");
    }

    #[test]
    fn subscribers_wake_only_on_a_change() {
        let health = Health::new();
        let mut subscriber = health.subscribe();
        subscriber.mark_unchanged();

        health.set_sink_ready(false);
        health.set_store_ready(false);
        health.set_validated(false);
        health.set_role(Role::Standby);
        health.set_lease_valid(false);
        health.set_delivering(false);
        assert!(!subscriber.has_changed().unwrap());

        health.set_sink_ready(true);
        assert!(subscriber.has_changed().unwrap());
        assert!(subscriber.borrow_and_update().sink_ready);
        assert!(!subscriber.has_changed().unwrap());

        health.set_role(Role::Active);
        assert!(subscriber.has_changed().unwrap());
    }

    #[test]
    fn clones_share_one_snapshot() {
        let health = Health::new();
        let clone = health.clone();
        clone.set_validated(true);
        assert!(health.snapshot().validated);
    }
}
