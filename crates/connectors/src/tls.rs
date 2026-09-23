//! TLS for the connectors that open their own connections: Kafka and NATS.
//! HTTP has its own, inside `ureq`, built the same way.
//!
//! One rule for all of them: `ring` as the cryptography provider (Settled
//! decision 16), and trust either the bundled public roots or, for a cluster
//! with a private certificate authority, the PEM file named by `ca_cert`. Not
//! the operating system's store, so a built artifact trusts the same things on
//! every machine it is copied to (Settled decision 42).

use etl_plugin_sdk::{ConnectorError, Context};
use std::sync::Arc;

/// A client configuration trusting `ca_cert` (resolved against the workspace)
/// if given, and the bundled public roots if not.
pub(crate) fn client_config(
    ca_cert: Option<&str>,
    context: &Context,
) -> Result<rustls::ClientConfig, ConnectorError> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::CertificateDer;

    let mut roots = rustls::RootCertStore::empty();
    match ca_cert {
        Some(written) => {
            let path = context.resolve(written);
            let unreadable = |reason: String| {
                ConnectorError::property("ca_cert", format!("{}: {reason}", path.display()))
            };
            let certificates = CertificateDer::pem_file_iter(&path)
                .map_err(|error| unreadable(error.to_string()))?;
            for certificate in certificates {
                let certificate = certificate.map_err(|error| unreadable(error.to_string()))?;
                roots
                    .add(certificate)
                    .map_err(|error| unreadable(error.to_string()))?;
            }
            if roots.is_empty() {
                return Err(unreadable("holds no PEM certificate".to_string()));
            }
        }
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
    }

    Ok(rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|error| ConnectorError::Data(format!("TLS could not be set up: {error}")))?
    .with_root_certificates(roots)
    .with_no_client_auth())
}
