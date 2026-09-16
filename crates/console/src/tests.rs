//! How the console is bound, and what it says about it.

use super::*;

fn at(address: &str) -> ServeOptions {
    ServeOptions {
        address: address.parse().expect("an address"),
        ..ServeOptions::default()
    }
}

#[test]
fn the_default_is_loopback() {
    // A console with no TLS and a bearer token is right for a machine
    // somebody is working on and wrong for a network, so reaching the network
    // has to be asked for.
    let options = ServeOptions::default();

    assert!(options.is_loopback());
    assert_eq!(options.address.port(), DEFAULT_PORT);
    assert!(options.warnings().is_empty());
}

#[test]
fn loopback_addresses_are_recognised() {
    assert!(at("127.0.0.1:8087").is_loopback());
    assert!(at("[::1]:8087").is_loopback());
    assert!(at("127.0.0.53:8087").is_loopback());
}

#[test]
fn binding_the_network_warns_and_says_why() {
    for address in ["0.0.0.0:8087", "192.168.1.10:8087"] {
        let options = at(address);

        assert!(!options.is_loopback(), "{address}");

        let warnings = options.warnings();
        assert_eq!(warnings.len(), 1, "{address}");

        // The warning has to say what the actual exposure is, not just that
        // something is unusual: the tokens cross the network in clear.
        assert!(warnings[0].contains("TLS"), "{}", warnings[0]);
        assert!(warnings[0].contains(address), "{}", warnings[0]);
        // And what to do instead.
        assert!(
            warnings[0].contains("proxy") || warnings[0].contains("tunnel"),
            "{}",
            warnings[0]
        );
    }
}

#[test]
fn the_link_carries_the_token() {
    // The one place a token belongs in a URL — the page takes it out of the
    // address bar on load.
    let link = at("127.0.0.1:9000").link("abc123");

    assert_eq!(link, "http://127.0.0.1:9000/?token=abc123");
}

#[test]
fn a_link_for_an_unspecified_bind_points_somewhere_a_browser_can_go() {
    // 0.0.0.0 is not an address to open. The person reading the line is on
    // this machine, so the link should be.
    let link = at("0.0.0.0:9000").link("abc123");

    assert!(link.starts_with("http://127.0.0.1:9000/"), "{link}");
}
