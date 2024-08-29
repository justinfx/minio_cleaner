# Minio Cache Cleaner

A service that monitors Minio [bucket notifications](https://min.io/docs/minio/linux/administration/monitoring/bucket-notifications.html) 
to track object access times, and apply cleanup policies.

## Status

This project is still WIP

- [x] pkg: Nats Jetstream event consumer
- [x] pkg: SQLite event store
- [ ] pkg: Minio cleanup policy manager
- [ ] CLI

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
