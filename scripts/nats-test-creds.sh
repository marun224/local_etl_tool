#!/bin/sh
# An operator-mode set-up for the test NATS server that checks `.creds` files:
# an operator, a system account, an account `APP` with JetStream enabled, and a
# user `etl` in it, made with `nsc` in a throwaway `nats-box` container. Run by
# scripts/test-services.ps1 with a Docker volume mounted at /creds, which the
# server then mounts. Test material only.
#
# Writes /creds/server.conf (the server's configuration, with the accounts
# preloaded into a memory resolver) and /creds/etl.creds (what the tests sign in
# with).
set -eu
export NSC_HOME=/tmp/nsc
export NKEYS_PATH=/tmp/nsc/keys
rm -rf /tmp/nsc /creds/*
mkdir -p /tmp/nsc

nsc env --store /tmp/nsc/stores >/dev/null 2>&1
nsc add operator --name etl-test --sys --generate-signing-key >/dev/null
nsc add account --name APP >/dev/null
nsc edit account --name APP --js-enable 1 >/dev/null
nsc add user --account APP --name etl >/dev/null

nsc generate config --mem-resolver --sys-account SYS > /creds/resolver.conf
nsc generate creds --account APP --name etl > /creds/etl.creds

cat > /creds/server.conf <<'CONF'
port: 4222
jetstream {
  store_dir: /tmp/js
}
include resolver.conf
CONF

chmod 644 /creds/*
echo "made: $(ls /creds | tr '\n' ' ')"
