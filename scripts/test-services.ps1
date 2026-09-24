<#
.SYNOPSIS
    Start the servers the verification tests run against: PostgreSQL, MySQL
    and MinIO (S3) from Phase 10c, Kafka from 10e, NATS from 10g, a Kinesis
    stand-in from 10h, an SQS stand-in from 10j, Google's Pub/Sub emulator
    from 10k and RabbitMQ from 10l, each in a throwaway container.

.DESCRIPTION
    The verification tests read these environment variables and skip each
    group when its variable is unset:

      ETL_TEST_POSTGRES   a libpq connection string
      ETL_TEST_MYSQL      a MySQL connection string
      ETL_TEST_S3         http://host:port of an S3-compatible endpoint, with
                          the bucket `etl-test` already created and the
                          credentials etl-test / etl-test-secret
      ETL_TEST_KAFKA      host:port of a Kafka broker (KRaft, one node) that
                          does not create topics on its own; tests make theirs
      ETL_TEST_KAFKA_SASL, ETL_TEST_KAFKA_TLS, ETL_TEST_KAFKA_SASL_TLS
                          the same broker's SASL_PLAINTEXT, SSL and SASL_SSL
                          listeners (user etl, password etl-secret, for PLAIN
                          and SCRAM-SHA-256/512)
      ETL_TEST_KAFKA_CA   the CA certificate the TLS listeners' certificate is
                          signed by, as a PEM file under target/test-services/
      ETL_TEST_NATS       nats://host:port of a NATS server with JetStream, open
      ETL_TEST_NATS_USERS, ETL_TEST_NATS_TOKEN, ETL_TEST_NATS_TLS, ETL_TEST_NATS_CREDS
                          NATS servers signing in by user etl / etl-secret, by
                          token etl-token, over TLS (Kafka's CA), and in
                          operator mode for a .creds file
      ETL_TEST_NATS_CREDS_FILE
                          that .creds file, under target/test-services/
      ETL_TEST_KINESIS    http://host:port of kinesis-mock, a Kinesis stand-in
                          that does not check signatures (the SigV4 unit tests
                          do); any credentials and region are accepted
      ETL_TEST_SQS        http://host:port of ElasticMQ, an SQS stand-in that
                          speaks SQS's JSON protocol and, like kinesis-mock,
                          accepts any signature
      ETL_TEST_PUBSUB     http://host:port of Google's Pub/Sub emulator, which
                          takes any project and checks no sign-in (the RS256
                          and token tests do, without it)
      ETL_TEST_RABBITMQ   amqp://etl:etl-secret@host:port/%2f of RabbitMQ 4.3
      ETL_TEST_RABBITMQ_TLS
                          the same broker's TLS listener, amqps://, with a
                          certificate signed by Kafka's CA (ETL_TEST_KAFKA_CA)
      ETL_TEST_RABBITMQ_HTTP
                          http://host:port of its management API, which the
                          engine's tests use to make queues and count them

    This script starts the containers on a private network, waits until each
    one answers, creates the bucket with MinIO's own `mc` client, and then
    prints the variables -- or, in GitHub Actions, writes them to
    $GITHUB_ENV so later steps see them.

    Ports are high and fixed so they are unlikely to collide with a real server
    on a developer's machine. Nothing is written outside Docker; -Stop removes
    all of it.

.EXAMPLE
    ./scripts/test-services.ps1           # start, wait, print the variables
    ./scripts/test-services.ps1 -Stop     # remove the containers and network
#>
[CmdletBinding()]
param(
    [switch] $Stop
)

# Not 'Stop': Windows PowerShell 5.1 turns anything a native command writes to
# stderr into a terminating error, and docker writes pull progress and "not
# found" notes there. Success is decided by exit codes instead, in Invoke-Docker.
$ErrorActionPreference = 'Continue'

$network = 'etl-test'
$kafkaSecrets = 'etl-test-kafka-secrets'
$natsCreds = 'etl-test-nats-creds'
$containers = @('etl-test-postgres', 'etl-test-mysql', 'etl-test-minio', 'etl-test-kafka',
    'etl-test-nats', 'etl-test-nats-users', 'etl-test-nats-token', 'etl-test-nats-tls',
    'etl-test-nats-creds', 'etl-test-kinesis', 'etl-test-sqs', 'etl-test-pubsub',
    'etl-test-rabbitmq')

function Invoke-Docker {
    $output = & docker @args 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($args -join ' ') failed ($LASTEXITCODE): $($output -join ' ')"
    }
}

function Remove-Everything {
    # Missing is fine here: this is the clean-up before a start as well.
    foreach ($name in $containers) {
        & docker rm -f $name 2>&1 | Out-Null
    }
    & docker network rm $network 2>&1 | Out-Null
    & docker volume rm $kafkaSecrets 2>&1 | Out-Null
    & docker volume rm $natsCreds 2>&1 | Out-Null
}

if ($Stop) {
    Remove-Everything
    Write-Host 'Stopped and removed the test services.'
    exit 0
}

& docker info --format '{{.ServerVersion}}' 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw 'Docker is not running. Start Docker Desktop (or the daemon) and run this again.'
}

# A clean start every time: a leftover container from an earlier run would
# hold the port, or hold yesterday's data.
Remove-Everything
Invoke-Docker network create $network

Write-Host 'Starting PostgreSQL, MySQL, MinIO, Kafka, NATS, Kinesis, SQS, Pub/Sub and RabbitMQ (the first run pulls the images)'

Invoke-Docker run -d --name etl-test-postgres --network $network -p 55432:5432 `
    -e POSTGRES_PASSWORD=etl postgres:16

Invoke-Docker run -d --name etl-test-mysql --network $network -p 53306:3306 `
    -e MYSQL_ROOT_PASSWORD=etl -e MYSQL_DATABASE=etl mysql:8.4

# From quay.io: MinIO no longer publishes to Docker Hub, where `minio/minio`
# now answers "repository does not exist" (found 2026-09-23). Any
# S3-compatible server would do; this one is only what the tests were
# first written against.
Invoke-Docker run -d --name etl-test-minio --network $network -p 59000:9000 `
    -e MINIO_ROOT_USER=etl-test -e MINIO_ROOT_PASSWORD=etl-test-secret `
    quay.io/minio/minio server /data

# The Kafka broker's certificates and SASL settings, made in a throwaway
# container of its own image into a Docker volume the broker then mounts. The
# script is read through `tr` so a CRLF checkout on Windows cannot break it.
Invoke-Docker volume create $kafkaSecrets
Invoke-Docker run --rm --user root -v "${kafkaSecrets}:/secrets" `
    -v "${PSScriptRoot}:/scripts:ro" --entrypoint sh apache/kafka:4.1.0 `
    -c 'tr -d ''\015'' < /scripts/kafka-test-secrets.sh | sh'

# Kafka 4.1 in KRaft mode, one node doing both jobs. A broker hands clients the
# address it advertises, so each listener the tests reach is advertised as
# 127.0.0.1 on its host port: EXTERNAL (plaintext, 59092), SASL
# (SASL_PLAINTEXT, 59094), TLS (SSL, 59095) and SASLTLS (SASL_SSL, 59096).
# INTERNAL is for the broker's own tools inside the container, which could not
# reach the host's ports. The listener names avoid "SSL://" and "SASL_" on
# purpose: the image's start-up script has its own rules for listeners named
# that way, and the settings here are Kafka's own properties instead. Topics
# are not created on demand, so a test that names a missing one sees the error
# a user would.
Invoke-Docker run -d --name etl-test-kafka --network $network `
    -p 59092:9092 -p 59094:9094 -p 59095:9095 -p 59096:9096 `
    -v "${kafkaSecrets}:/etc/kafka/secrets:ro" `
    -e KAFKA_NODE_ID=1 `
    -e 'KAFKA_PROCESS_ROLES=broker,controller' `
    -e 'KAFKA_LISTENERS=EXTERNAL://:9092,INTERNAL://:19092,CONTROLLER://:9093,SASL://:9094,TLS://:9095,SASLTLS://:9096' `
    -e 'KAFKA_ADVERTISED_LISTENERS=EXTERNAL://127.0.0.1:59092,INTERNAL://localhost:19092,SASL://127.0.0.1:59094,TLS://127.0.0.1:59095,SASLTLS://127.0.0.1:59096' `
    -e 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,SASL:SASL_PLAINTEXT,TLS:SSL,SASLTLS:SASL_SSL' `
    -e 'KAFKA_SASL_ENABLED_MECHANISMS=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512' `
    -e 'KAFKA_OPTS=-Djava.security.auth.login.config=/etc/kafka/secrets/jaas.conf' `
    -e KAFKA_SSL_KEYSTORE_LOCATION=/etc/kafka/secrets/server.p12 `
    -e KAFKA_SSL_KEYSTORE_TYPE=PKCS12 `
    -e KAFKA_SSL_KEYSTORE_PASSWORD=etl-test-secret `
    -e KAFKA_SSL_KEY_PASSWORD=etl-test-secret `
    -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL `
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER `
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 `
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 `
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 `
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 `
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false `
    apache/kafka:4.1.0

# NATS 2.11 with JetStream, five small servers, one per way of signing in,
# because a server takes one kind of client authentication at a time. Each has
# its monitoring port inside the container for the readiness probe. TLS reuses
# the certificate made for Kafka above, whose names include 127.0.0.1. The
# operator-mode server for `.creds` gets its operator, account and user from
# `nsc` in a throwaway nats-box container, into a volume it then mounts.
$nats = 'nats:2.11-alpine'
Invoke-Docker run -d --name etl-test-nats --network $network -p 54222:4222 $nats -js -m 8222
Invoke-Docker run -d --name etl-test-nats-users --network $network -p 54223:4222 $nats `
    -js -m 8222 --user etl --pass etl-secret
Invoke-Docker run -d --name etl-test-nats-token --network $network -p 54224:4222 $nats `
    -js -m 8222 --auth etl-token
Invoke-Docker run -d --name etl-test-nats-tls --network $network -p 54225:4222 `
    -v "${kafkaSecrets}:/certs:ro" $nats `
    -js -m 8222 --tls --tlscert /certs/server.pem --tlskey /certs/server.key
Invoke-Docker volume create $natsCreds
Invoke-Docker run --rm --user root -v "${natsCreds}:/creds" -v "${PSScriptRoot}:/scripts:ro" `
    natsio/nats-box:0.18.0 sh -c 'tr -d ''\015'' < /scripts/nats-test-creds.sh | sh'
Invoke-Docker run -d --name etl-test-nats-creds --network $network -p 54226:4222 `
    -v "${natsCreds}:/creds:ro" $nats -c /creds/server.conf -m 8222

# kinesis-mock, the Kinesis implementation LocalStack itself runs inside, on
# its own. Plain HTTP on 4568 (it serves TLS on 4567, with a certificate of its
# own that the tests do not need). Streams are created by the tests; a
# create, split or merge takes half a second to settle, as on AWS it takes
# longer.
Invoke-Docker run -d --name etl-test-kinesis --network $network -p 54568:4568 `
    -e KINESIS_MOCK_PLAIN_PORT=4568 -e KINESIS_MOCK_TLS_PORT=4567 `
    -e CREATE_STREAM_DURATION=200ms -e SPLIT_SHARD_DURATION=200ms `
    -e MERGE_SHARDS_DURATION=200ms -e SHARD_LIMIT=1000 -e LOG_LEVEL=WARN `
    ghcr.io/etspaceman/kinesis-mock:0.4.13

# ElasticMQ, an SQS implementation in one small native binary. It answers the
# JSON protocol the connector speaks; queues are created by the tests.
Invoke-Docker run -d --name etl-test-sqs --network $network -p 59324:9324 `
    softwaremill/elasticmq-native:1.7.1

# Google's Pub/Sub emulator, from the gcloud image that carries the emulators
# (445 MB). It keeps everything in memory, takes any project, and checks no
# sign-in; topics and subscriptions are created by the tests.
Invoke-Docker run -d --name etl-test-pubsub --network $network -p 58085:8085 `
    gcr.io/google.com/cloudsdktool/google-cloud-cli:586.0.0-emulators `
    gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=etl-test

# RabbitMQ 4.3, a plain listener and a TLS one with the certificate made for
# Kafka above, and the management plugin (bundled in the image) for its HTTP
# API. The TLS settings and the plugin list are written by the container's own
# shell before the server starts, so no file from this checkout (and its line
# endings) is involved. User etl / etl-secret; guest may only sign in from
# inside the container. Ports 5767x: Windows reserves some ranges near 55672
# for itself (found on this machine), and a reserved port cannot be published.
$rabbitTls = @(
    'listeners.ssl.default = 5671',
    'ssl_options.cacertfile = /certs/ca.pem',
    'ssl_options.certfile = /certs/server.pem',
    'ssl_options.keyfile = /certs/server.key',
    'ssl_options.verify = verify_none',
    'ssl_options.fail_if_no_peer_cert = false'
) | ForEach-Object { "'$_'" }   # single quotes: PowerShell 5.1 mangles nested double ones
Invoke-Docker run -d --name etl-test-rabbitmq --network $network `
    -p 57672:5672 -p 57671:5671 -p 57673:15672 -v "${kafkaSecrets}:/certs:ro" `
    -e RABBITMQ_DEFAULT_USER=etl -e RABBITMQ_DEFAULT_PASS=etl-secret `
    --entrypoint sh rabbitmq:4.3-alpine -c `
    "printf '%s\n' $($rabbitTls -join ' ') > /etc/rabbitmq/conf.d/20-etl-tls.conf && echo '[rabbitmq_management].' > /etc/rabbitmq/enabled_plugins && exec docker-entrypoint.sh rabbitmq-server"

function Wait-For([string] $what, [scriptblock] $probe, [int] $seconds = 180) {
    $deadline = (Get-Date).AddSeconds($seconds)
    while ((Get-Date) -lt $deadline) {
        & $probe 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  $what is ready"
            return
        }
        Start-Sleep -Seconds 2
    }
    throw "$what did not become ready within $seconds seconds; see 'docker logs' for it"
}

Wait-For 'PostgreSQL' { docker exec etl-test-postgres pg_isready -U postgres }
# mysqladmin ping answers before the server accepts real queries during first
# start-up, so the probe is a query.
Wait-For 'MySQL' { docker exec etl-test-mysql mysql -uroot -petl -e 'SELECT 1' etl }

# The bucket, made with MinIO's own client from a companion container on the
# same network: DuckDB cannot create a bucket, and this works the same on
# Docker Desktop and on a Linux runner.
Wait-For 'MinIO' {
    docker run --rm --network $network --entrypoint sh quay.io/minio/mc -c `
        'mc alias set m http://etl-test-minio:9000 etl-test etl-test-secret && mc mb --ignore-existing m/etl-test'
}

Wait-For 'Kafka' {
    docker exec etl-test-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
}

# SCRAM users live in the cluster's metadata rather than in a file, so they are
# added once the broker answers, one mechanism per request: Kafka refuses to
# alter one user's credentials twice in the same request. PLAIN's user came
# from jaas.conf.
foreach ($mechanism in 'SCRAM-SHA-256', 'SCRAM-SHA-512') {
    Invoke-Docker exec etl-test-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:19092 `
        --alter --entity-type users --entity-name etl `
        --add-config "$mechanism=[password=etl-secret]"
}

foreach ($name in 'etl-test-nats', 'etl-test-nats-users', 'etl-test-nats-token',
                  'etl-test-nats-tls', 'etl-test-nats-creds') {
    Wait-For $name {
        docker exec $name wget -q -O /dev/null 'http://127.0.0.1:8222/healthz?js-enabled-only=true'
    }
}

# A request with any signature is answered; a stream list means it is up.
Wait-For 'Kinesis' {
    $request = @{
        Uri = 'http://127.0.0.1:54568/'; Method = 'Post'; UseBasicParsing = $true
        ContentType = 'application/x-amz-json-1.1'; Body = '{}'; TimeoutSec = 5
        Headers = @{ 'X-Amz-Target' = 'Kinesis_20131202.ListStreams'
                     'Authorization' = 'AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/kinesis/aws4_request, SignedHeaders=host, Signature=0'
                     'X-Amz-Date' = '20260101T000000Z' }
    }
    try { Invoke-WebRequest @request | Out-Null; $global:LASTEXITCODE = 0 } catch { $global:LASTEXITCODE = 1 }
}

# The same for SQS: a queue list means it is up.
Wait-For 'SQS' {
    $request = @{
        Uri = 'http://127.0.0.1:59324/'; Method = 'Post'; UseBasicParsing = $true
        ContentType = 'application/x-amz-json-1.0'; Body = '{}'; TimeoutSec = 5
        Headers = @{ 'X-Amz-Target' = 'AmazonSQS.ListQueues'
                     'Authorization' = 'AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=0'
                     'X-Amz-Date' = '20260101T000000Z' }
    }
    try { Invoke-WebRequest @request | Out-Null; $global:LASTEXITCODE = 0 } catch { $global:LASTEXITCODE = 1 }
}

# The emulator answers a topic list once it is up.
Wait-For 'Pub/Sub' {
    try {
        Invoke-WebRequest -Uri 'http://127.0.0.1:58085/v1/projects/etl-test/topics' `
            -UseBasicParsing -TimeoutSec 5 | Out-Null
        $global:LASTEXITCODE = 0
    } catch { $global:LASTEXITCODE = 1 }
}

# Both listeners accepting connections, not merely the node up.
Wait-For 'RabbitMQ' { docker exec etl-test-rabbitmq rabbitmq-diagnostics -q check_port_connectivity }

# The CA certificate, for the tests' `ca_cert`. Under target/, which git ignores.
$caDirectory = Join-Path $PSScriptRoot '../target/test-services'
New-Item -ItemType Directory -Force $caDirectory | Out-Null
$kafkaCa = Join-Path (Resolve-Path $caDirectory) 'kafka-ca.pem'
Invoke-Docker cp etl-test-kafka:/etc/kafka/secrets/ca.pem $kafkaCa
$natsCredsFile = Join-Path (Resolve-Path $caDirectory) 'nats-etl.creds'
Invoke-Docker cp etl-test-nats-creds:/creds/etl.creds $natsCredsFile

$variables = [ordered]@{
    ETL_TEST_POSTGRES = 'host=127.0.0.1 port=55432 user=postgres password=etl dbname=postgres'
    ETL_TEST_MYSQL    = 'host=127.0.0.1 port=53306 user=root passwd=etl database=etl'
    ETL_TEST_S3       = 'http://127.0.0.1:59000'
    ETL_TEST_KAFKA    = '127.0.0.1:59092'
    ETL_TEST_KAFKA_SASL     = '127.0.0.1:59094'
    ETL_TEST_KAFKA_TLS      = '127.0.0.1:59095'
    ETL_TEST_KAFKA_SASL_TLS = '127.0.0.1:59096'
    ETL_TEST_KAFKA_CA       = $kafkaCa
    ETL_TEST_NATS           = 'nats://127.0.0.1:54222'
    ETL_TEST_NATS_USERS     = 'nats://127.0.0.1:54223'
    ETL_TEST_NATS_TOKEN     = 'nats://127.0.0.1:54224'
    ETL_TEST_NATS_TLS       = 'nats://127.0.0.1:54225'
    ETL_TEST_NATS_CREDS     = 'nats://127.0.0.1:54226'
    ETL_TEST_NATS_CREDS_FILE = $natsCredsFile
    ETL_TEST_KINESIS        = 'http://127.0.0.1:54568'
    ETL_TEST_SQS            = 'http://127.0.0.1:59324'
    ETL_TEST_PUBSUB         = 'http://127.0.0.1:58085'
    ETL_TEST_RABBITMQ       = 'amqp://etl:etl-secret@127.0.0.1:57672/%2f'
    ETL_TEST_RABBITMQ_TLS   = 'amqps://etl:etl-secret@127.0.0.1:57671/%2f'
    ETL_TEST_RABBITMQ_HTTP  = 'http://127.0.0.1:57673'
}

if ($env:GITHUB_ENV) {
    foreach ($name in $variables.Keys) {
        Add-Content -Path $env:GITHUB_ENV -Value "$name=$($variables[$name])"
    }
    Write-Host "Wrote $($variables.Keys -join ', ') to GITHUB_ENV."
} else {
    Write-Host ''
    Write-Host 'Set these, then run: cargo test --workspace'
    Write-Host ''
    foreach ($name in $variables.Keys) {
        Write-Host "`$env:$name = '$($variables[$name])'"
    }
}
