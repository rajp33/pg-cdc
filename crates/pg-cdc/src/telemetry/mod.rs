use crate::config::TelemetryConfig;

pub const OTLP_ENDPOINT_VARIABLE: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";

#[derive(Debug, Clone, PartialEq)]
pub enum TelemetryPlan {
    Disabled,
    Enabled { endpoint: String, sample_ratio: f64 },
}

pub fn plan(config: &TelemetryConfig, env_endpoint: Option<&str>) -> TelemetryPlan {
    let configured = config.otlp_endpoint.as_deref().filter(|e| !e.is_empty());
    let environment = env_endpoint.filter(|e| !e.is_empty());
    match configured.or(environment) {
        Some(endpoint) => TelemetryPlan::Enabled {
            endpoint: endpoint.to_owned(),
            sample_ratio: config.trace_sample_ratio,
        },
        None => TelemetryPlan::Disabled,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(endpoint: Option<&str>, ratio: f64) -> TelemetryConfig {
        TelemetryConfig {
            otlp_endpoint: endpoint.map(str::to_owned),
            trace_sample_ratio: ratio,
        }
    }

    #[test]
    fn telemetry_is_disabled_unless_an_endpoint_is_set() {
        assert_eq!(plan(&config(None, 0.01), None), TelemetryPlan::Disabled);
        assert_eq!(
            plan(&config(Some(""), 0.01), Some("")),
            TelemetryPlan::Disabled
        );
        assert_eq!(
            plan(&config(None, 0.25), Some("http://env:4317")),
            TelemetryPlan::Enabled {
                endpoint: "http://env:4317".to_owned(),
                sample_ratio: 0.25,
            }
        );
        assert_eq!(
            plan(
                &config(Some("http://cfg:4317"), 1.0),
                Some("http://env:4317")
            ),
            TelemetryPlan::Enabled {
                endpoint: "http://cfg:4317".to_owned(),
                sample_ratio: 1.0,
            }
        );
        assert_eq!(
            plan(&config(Some(""), 0.5), Some("http://env:4317")),
            TelemetryPlan::Enabled {
                endpoint: "http://env:4317".to_owned(),
                sample_ratio: 0.5,
            }
        );
    }

    #[test]
    fn the_endpoint_variable_is_the_standard_otlp_one() {
        assert_eq!(OTLP_ENDPOINT_VARIABLE, "OTEL_EXPORTER_OTLP_ENDPOINT");
    }
}
