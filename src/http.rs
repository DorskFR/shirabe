//! Outbound HTTP client construction. reqwest is built without a crypto
//! provider so rustls stays on `ring`, the provider sqlx also links; every
//! client goes through here so the provider is installed before TLS is used.

use std::sync::Once;

fn install_ring() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

pub fn client_builder() -> reqwest::ClientBuilder {
    install_ring();
    reqwest::Client::builder()
}

/// A client with reqwest's defaults, the equivalent of `reqwest::Client::new()`.
#[must_use]
pub fn client() -> reqwest::Client {
    client_builder().build().expect("default reqwest client")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clients_build_on_the_ring_provider() {
        let _ = client();
        let provider = rustls::crypto::CryptoProvider::get_default().expect("provider installed");
        assert_eq!(format!("{:?}", provider.key_provider), "Ring");
        assert!(client_builder().build().is_ok());
    }
}
