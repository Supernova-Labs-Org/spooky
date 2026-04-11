use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::fs;

pub fn load_tls(
    cert_path: &str,
    key_path: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), std::io::Error> {
    let cert_bytes = fs::read(cert_path)?;
    let key_bytes = fs::read(key_path)?;

    let certs = vec![CertificateDer::from(cert_bytes)];
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key_bytes));

    Ok((certs, key))
}
