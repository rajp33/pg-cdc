use aws_config::{BehaviorVersion, ConfigLoader, Region};
use aws_smithy_http_client::tls::{Provider, rustls_provider::CryptoMode};

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
