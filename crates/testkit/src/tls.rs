use std::path::{Path, PathBuf};

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use tempfile::TempDir;

use crate::error::TestkitError;

#[derive(Debug)]
pub(crate) struct TestCertificates {
    server_pem: String,
    server_key_pem: String,
    ca_file: PathBuf,
    _dir: TempDir,
}

impl TestCertificates {
    pub(crate) fn generate() -> Result<Self, TestkitError> {
        let ca_key = KeyPair::generate()?;
        let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "pg-cdc test CA");
        let ca_cert = ca_params.self_signed(&ca_key)?;
        let issuer = Issuer::new(ca_params, ca_key);
        let server_key = KeyPair::generate()?;
        let server_cert = CertificateParams::new(vec!["localhost".to_owned()])?
            .signed_by(&server_key, &issuer)?;
        let dir = tempfile::tempdir()?;
        let ca_file = dir.path().join("ca.pem");
        std::fs::write(&ca_file, ca_cert.pem())?;
        Ok(Self {
            server_pem: server_cert.pem(),
            server_key_pem: server_key.serialize_pem(),
            ca_file,
            _dir: dir,
        })
    }

    pub(crate) fn server_pem(&self) -> &str {
        &self.server_pem
    }

    pub(crate) fn server_key_pem(&self) -> &str {
        &self.server_key_pem
    }

    pub(crate) fn ca_file(&self) -> &Path {
        &self.ca_file
    }
}
