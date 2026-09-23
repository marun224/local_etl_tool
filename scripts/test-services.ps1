<#
.SYNOPSIS
    Start the servers the verification tests run against: PostgreSQL, MySQL
    and MinIO (S3) from Phase 10c, and Kafka from 10e, each in a throwaway
    container.

.DESCRIPTION
    The verification tests read four environment variables and skip each
    group when its variable is unset:

      ETL_TEST_POSTGRES   a libpq connection string
      ETL_TEST_MYSQL      a MySQL connection string
      ETL_TEST_S3         http://host:port of an S3-compatible endpoint, with
                          the bucket `etl-test` already created and the
                          credentials etl-test / etl-test-secret
      ETL_TEST_KAFKA      host:port of a Kafka broker (KRaft, one node) that
                          does not create topics on its own; tests make theirs

    This script starts the four containers on a private network, waits until
    each one answers, creates the bucket with MinIO's own `mc` client, and then
    prints the four variables -- or, in GitHub Actions, writes them to
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
$containers = @('etl-test-postgres', 'etl-test-mysql', 'etl-test-minio', 'etl-test-kafka')

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

Write-Host 'Starting PostgreSQL, MySQL, MinIO and Kafka (the first run pulls the images)'

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

# Kafka 4.1 in KRaft mode, one node doing both jobs. Two listeners, because a
# broker hands clients the address it advertises: EXTERNAL is what the tests
# reach from the host, as 127.0.0.1:59092, and INTERNAL is for the broker's own
# tools inside the container, which could not reach the host's port. Topics
# are not created on demand, so a test that names a missing one sees the error
# a user would.
Invoke-Docker run -d --name etl-test-kafka --network $network -p 59092:9092 `
    -e KAFKA_NODE_ID=1 `
    -e 'KAFKA_PROCESS_ROLES=broker,controller' `
    -e 'KAFKA_LISTENERS=EXTERNAL://:9092,INTERNAL://:19092,CONTROLLER://:9093' `
    -e 'KAFKA_ADVERTISED_LISTENERS=EXTERNAL://127.0.0.1:59092,INTERNAL://localhost:19092' `
    -e 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT' `
    -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL `
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER `
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 `
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 `
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 `
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 `
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false `
    apache/kafka:4.1.0

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

$variables = [ordered]@{
    ETL_TEST_POSTGRES = 'host=127.0.0.1 port=55432 user=postgres password=etl dbname=postgres'
    ETL_TEST_MYSQL    = 'host=127.0.0.1 port=53306 user=root passwd=etl database=etl'
    ETL_TEST_S3       = 'http://127.0.0.1:59000'
    ETL_TEST_KAFKA    = '127.0.0.1:59092'
}

if ($env:GITHUB_ENV) {
    foreach ($name in $variables.Keys) {
        Add-Content -Path $env:GITHUB_ENV -Value "$name=$($variables[$name])"
    }
    Write-Host 'Wrote ETL_TEST_POSTGRES, ETL_TEST_MYSQL, ETL_TEST_S3 and ETL_TEST_KAFKA to GITHUB_ENV.'
} else {
    Write-Host ''
    Write-Host 'Set these, then run: cargo test --workspace'
    Write-Host ''
    foreach ($name in $variables.Keys) {
        Write-Host "`$env:$name = '$($variables[$name])'"
    }
}
