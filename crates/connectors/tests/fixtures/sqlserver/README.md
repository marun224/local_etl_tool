# TLS material for the SQL Server fixture

For `src/sqlserver/tests.rs` only: the local TDS fixture encrypts with these, so the
connector's `ca_cert` and `trust_server_certificate` are tested without a SQL Server. Nothing
here protects anything.

| File | What |
|---|---|
| `ca.pem` | A certificate authority, `CN=etl test SQL Server CA`. Its key was deleted after signing. |
| `server.pem`, `server.key` | The fixture's certificate for `localhost` and `127.0.0.1`, signed by `ca.pem`; its PKCS#8 key. |
| `other-ca.pem` | An unrelated authority, `CN=some other CA`, for the test that a server it did not sign is refused. Its key was deleted too. |

All are EC P-256 and valid for 100 years from 2026-09-24. Made with OpenSSL 3.5.7:

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -keyout ca.key -out ca.pem \
  -days 36500 -subj "/CN=etl test SQL Server CA" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign"
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -keyout server.key -out server.csr \
  -subj "/CN=localhost"
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out server.pem \
  -days 36500 -extfile server.ext   # CA:FALSE, digitalSignature, serverAuth, DNS:localhost, IP:127.0.0.1
openssl pkcs8 -topk8 -nocrypt -in server.key -out server.key   # via a temporary file
# other-ca.pem: as ca.pem, with -subj "/CN=some other CA"
```
