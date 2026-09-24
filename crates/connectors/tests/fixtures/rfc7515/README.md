# RFC 7515, appendix A.2: an RS256 example

`a2.json` is the example in [RFC 7515](https://www.rfc-editor.org/rfc/rfc7515.txt) (JSON Web
Signature), appendix A.2, "Example JWS Using RSASSA-PKCS1-v1_5 SHA-256", taken from the RFC's
text on 2026-09-24: the RSA key as a JWK, the protected header, the payload (with the CRLFs
the RFC puts in it), the signing input, the signature as the RFC's list of bytes, and the whole
JWS in compact form. Nothing was changed but the layout: the RFC breaks long values across
lines for display, and those breaks were removed. Code components in RFCs are licensed under
the Simplified BSD License (the IETF Trust's Legal Provisions, section 4).

`crates/connectors/src/gcp/tests.rs` builds the key from the JWK, signs the signing input with
the project's own RS256 (`gcp::rs256`, over `ring`) and compares the signature byte for byte,
and encodes the header and payload and compares those. RSASSA-PKCS1-v1_5 is deterministic, so
a correct signer must produce exactly these bytes. This is the proof that signing in to Google
Cloud (Settled decision 64) signs as the standard says: the Pub/Sub emulator checks no sign-in,
and no Google Cloud project is used (decision 70).

The key is the RFC's published example key. It signs nothing real.
