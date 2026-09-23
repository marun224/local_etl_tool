# AWS Signature Version 4 test suite

These 38 cases are AWS's own SigV4 test suite, copied unchanged from
[awslabs/aws-c-auth](https://github.com/awslabs/aws-c-auth), directory
`tests/aws-signing-test-suite/v4`, at commit `c4bc791ac6985eedb503e882cd450cc5b344c2f2`
(fetched 2026-09-24). They are licensed under the Apache License 2.0; the licence is
`LICENSE` beside this file.

Each case holds a raw HTTP request (`request.txt`), the signing context (`context.json`:
credentials, region, service, time, and whether to normalise the path or sign the body), and
what a correct signer produces: `header-canonical-request.txt`, `header-string-to-sign.txt`
and `header-signature.txt`. Only the header-signing files were copied; this project does not
sign by query string.

`crates/connectors/src/aws/tests.rs` signs every case and compares all three outputs byte for
byte. This is the proof that the project's own signing (Settled decision 47) is AWS's: the
Kinesis test server does not check signatures, and no real AWS account is used (decision 56).
