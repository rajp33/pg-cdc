use std::path::Path;
use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};

use super::{ConnectError, TlsConfig};
use crate::config::TlsMode;

pub(super) fn client_config(tls: &TlsConfig) -> Result<Option<ClientConfig>, ConnectError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()?;
    let config = match tls.mode {
        TlsMode::Disable => return Ok(None),
        TlsMode::Require => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCertificate { provider }))
            .with_no_client_auth(),
        TlsMode::VerifyCa => {
            let roots = Arc::new(root_store(tls.ca_file.as_deref())?);
            let inner = WebPkiServerVerifier::builder_with_provider(roots, provider).build()?;
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(IgnoreHostName { inner }))
                .with_no_client_auth()
        }
        TlsMode::VerifyFull => builder
            .with_root_certificates(root_store(tls.ca_file.as_deref())?)
            .with_no_client_auth(),
    };
    Ok(Some(config))
}

fn root_store(ca_file: Option<&Path>) -> Result<RootCertStore, ConnectError> {
    let Some(path) = ca_file else {
        return Ok(webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect());
    };
    let ca_error = |source| ConnectError::CaFile {
        path: path.to_owned(),
        source,
    };
    let mut store = RootCertStore::empty();
    for certificate in CertificateDer::pem_file_iter(path).map_err(ca_error)? {
        store.add(certificate.map_err(ca_error)?)?;
    }
    if store.is_empty() {
        return Err(ConnectError::EmptyCaFile(path.to_owned()));
    }
    Ok(store)
}

#[derive(Debug)]
struct AcceptAnyCertificate {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Debug)]
struct IgnoreHostName {
    inner: Arc<WebPkiServerVerifier>,
}

impl ServerCertVerifier for IgnoreHostName {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}
