# Minio Cache Cleaner

[![Build](https://github.com/justinfx/minio_cleaner/workflows/Build%20and%20Release/badge.svg)](https://github.com/justinfx/minio_cleaner/actions?query=workflow%3A%22Build+and+Release%22)
[![Go Report Card](https://goreportcard.com/badge/github.com/justinfx/minio_cleaner)](https://goreportcard.com/report/github.com/justinfx/minio_cleaner)

A service that monitors Minio [bucket notifications](https://min.io/docs/minio/linux/administration/monitoring/bucket-notifications.html) 
to track object access times, and apply cleanup policies.

## Status

- [x] pkg: Nats Jetstream event consumer
- [x] pkg: SQLite event store
- [x] pkg: Minio cleanup policy manager ([#2](https://github.com/justinfx/minio_cleaner/issues/2))
- [x] CLI ([#1](https://github.com/justinfx/minio_cleaner/issues/1))
- [x] tests: end-to-end tests
- [ ] tests: Docker compose configuration

## Motivation

Minio provides the ability to define a time-to-live policy at a Bucket level. These TTLs have a minimum 
granularity of 1 day, in keeping with S3 compatability. Furthermore, it does not provide any kind of 
object access time tracking. Any need for more granular object expirations, based either on TTL (created time) 
or an access time, must be performed by an external process.

The motivation for this project came from using Minio as a distributed build system object cache for large
C++ projects. The storage space behind the Minio bucket can fill quite quickly, given enough projects with
enough variations pushing objects, and the need for an eviction policy by access time was needed to reduce 
the storage size more quickly (and smartly) that the longer and more naive bucket TTL policies.

## Architecture

By enabling the [NATS Endpoint](https://min.io/docs/minio/linux/administration/monitoring/publish-events-to-nats.html#minio-bucket-notifications-publish-nats) 
in the Minio cluster, the service can create a Nats Jetstream consumer to track object activity.

Bucket events are stored in an SQLite database, and updated by access or deletions. 

A cleanup policy can be defined, with access to the Minio cluster. Based on cluster size thresholds, the
cleanup manager can begin deleting objects with the oldest access time, until the cluster size reduces to
an acceptable value.

## Set up

1. Create a stream in your Nats JetStream service
2. Set up your Minio cluster with an Event Destination, pointing at the Nats subject and stream (GET/PUT/DELETE)
3. Enable the event for 1 or more buckets
4. Define minio_clean config.toml with the details of your minio and nats endpoints, and the bucket cleanup policies

## Testing

WIP (pending a docker compose)

Start minio

```
docker run -it --rm \
    -p 9000:9000 \
    -p 9010:9010 \
    -v /tmp/minio/data:/tmp/data:rw \
    minio/minio server --console-address :9010 /tmp/data 
```

Start Nats Jetstream

```
docker run -p 4222:4222 nats -js
```

Download [Nats CLI tool](https://github.com/nats-io/natscli/releases), and add a stream

```
# create new stream
nats stream add --defaults --subjects='minio.event' MINIO
# listen to raw events (for debugging)
nats subscribe minio.event --stream MINIO --new
```

Log into Minio console via http://localhost:9000/
* Configure Nats Event Destination (subject: 'minio.event' to match our consumer)
* Create a bucket
* Enable events (put, get, delete) for new bucket, to Nats endpoint.

At this point, any bucket write/read/delete operations should trigger events into Nats, and then into
the cleaner service.
