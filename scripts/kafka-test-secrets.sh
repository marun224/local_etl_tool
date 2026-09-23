#!/bin/sh
# The certificates and SASL settings for the test Kafka broker, made in a
# throwaway container of the broker's own image (it has openssl), so the machine
# running the tests needs neither openssl nor a JDK. Run by
# scripts/test-services.ps1 with a Docker volume mounted at /secrets, which the
# broker then mounts at /etc/kafka/secrets.
#
# Test material only: a CA made a moment ago, a server certificate for
# 127.0.0.1 and localhost signed by it, and fixed passwords. The CA's
# certificate is what the tests pass as `ca_cert`.
set -eu
cd /secrets
rm -f ./*

# A CA, and a server certificate it signs. The broker is reached as 127.0.0.1,
# so that is in the certificate as an IP address; rustls checks it.
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=etl-test-ca" \
    -keyout ca.key -out ca.pem \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
    -keyout server.key -out server.csr 2>/dev/null
printf '%s\n' \
    'subjectAltName=DNS:localhost,IP:127.0.0.1' \
    'basicConstraints=CA:FALSE' \
    'keyUsage=digitalSignature,keyEncipherment' \
    'extendedKeyUsage=serverAuth' > server.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
    -days 3650 -out server.pem -extfile server.ext 2>/dev/null

# Kafka reads its key from a PKCS12 keystore.
openssl pkcs12 -export -in server.pem -inkey server.key -certfile ca.pem \
    -name kafka -out server.p12 -passout pass:etl-test-secret

# SASL: user `etl` with password `etl-secret`. PLAIN takes it from here; SCRAM
# users are added after the broker starts, with kafka-configs.sh.
cat > jaas.conf <<'JAAS'
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
    user_etl="etl-secret";
  org.apache.kafka.common.security.scram.ScramLoginModule required;
};
JAAS

chmod 644 ./*
echo "made: $(ls | tr '\n' ' ')"
