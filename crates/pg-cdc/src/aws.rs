use aws_config::{BehaviorVersion, ConfigLoader, Region};
use aws_smithy_http_client::tls::{Provider, rustls_provider::CryptoMode};

use crate::metrics::ThrottleReason;

const THROTTLING_CODES: [&str; 16] = [
    "Throttling",
    "ThrottlingException",
    "Throttled",
    "ThrottledException",
    "RequestThrottled",
    "RequestThrottledException",
    "TooManyRequestsException",
    "ProvisionedThroughputExceededException",
    "TransactionInProgressException",
    "RequestLimitExceeded",
    "BandwidthLimitExceeded",
    "LimitExceededException",
    "SlowDown",
    "PriorRequestNotComplete",
    "EC2ThrottledException",
    "TooManyRequests",
];

pub(crate) fn aws_loader(region: Option<String>) -> ConfigLoader {
    let http_client = aws_smithy_http_client::Builder::new()
        .tls_provider(Provider::Rustls(CryptoMode::Ring))
        .build_https();
    let loader = aws_config::defaults(BehaviorVersion::latest()).http_client(http_client);
    match region {
        Some(region) => loader.region(Region::new(region)),
        None => loader,
    }
}

pub(crate) fn throttle_reason(code: Option<&str>, status: Option<u16>) -> Option<ThrottleReason> {
    match code {
        Some(code) if code.starts_with("KMSThrottling") => Some(ThrottleReason::KmsThrottling),
        Some(code) if THROTTLING_CODES.contains(&code) => Some(ThrottleReason::Throttling),
        Some("ServiceUnavailable" | "ServiceUnavailableException") => {
            Some(ThrottleReason::ServiceUnavailable)
        }
        _ => match status {
            Some(429) => Some(ThrottleReason::Throttling),
            Some(503) => Some(ThrottleReason::ServiceUnavailable),
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throttling_codes_and_statuses_map_to_their_reason() {
        let cases = [
            (
                Some("Throttling"),
                Some(400),
                Some(ThrottleReason::Throttling),
            ),
            (
                Some("Throttled"),
                Some(429),
                Some(ThrottleReason::Throttling),
            ),
            (
                Some("ThrottlingException"),
                None,
                Some(ThrottleReason::Throttling),
            ),
            (
                Some("SlowDown"),
                Some(503),
                Some(ThrottleReason::Throttling),
            ),
            (
                Some("KMSThrottling"),
                Some(400),
                Some(ThrottleReason::KmsThrottling),
            ),
            (
                Some("KMSThrottlingException"),
                None,
                Some(ThrottleReason::KmsThrottling),
            ),
            (
                Some("ServiceUnavailable"),
                Some(503),
                Some(ThrottleReason::ServiceUnavailable),
            ),
            (None, Some(429), Some(ThrottleReason::Throttling)),
            (None, Some(503), Some(ThrottleReason::ServiceUnavailable)),
            (
                Some("InternalError"),
                Some(503),
                Some(ThrottleReason::ServiceUnavailable),
            ),
            (Some("InvalidParameter"), Some(400), None),
            (Some("KMSAccessDenied"), Some(400), None),
            (Some("InternalError"), Some(500), None),
            (None, Some(500), None),
            (None, None, None),
        ];
        for (code, status, reason) in cases {
            assert_eq!(throttle_reason(code, status), reason, "{code:?} {status:?}");
        }
    }
}
