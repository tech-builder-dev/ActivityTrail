# Activity Trail

Activity Trail is an open-source service a team clones and hosts in its own environment. Publishing applications send one event per changed field to a Kafka topic. Activity Trail stores one row per event and serves that history through a read API. A support tool uses the API, for one object in one application, to answer who changed a field, when it changed, the old value, and the new value.

The repository name is `activitytrail`. The product name is Activity Trail. Version 1 is one Java 21 Spring Boot process, built with Gradle. The runnable artifact is `build/libs/activitytrail.jar`.

```sh
./gradlew bootJar
java -jar build/libs/activitytrail.jar --spring.profiles.active=local
```

Start that command from the repository root. The profile selects the environment: `local`, `qal`, `e2e`, or `prod`. The process serves HTTP and starts the Kafka consumer. A container image is `docker build -t activitytrail .` after the JAR exists. Pass the same profile when the container starts.

The default PostgreSQL database can be created from the scripts in [`db/postgres`](db/postgres) before the JAR starts. Flyway also applies `V1__schema.sql` on startup when the tables are not already there.

| Document | What it decides |
| --- | --- |
| [docs/prd.md](docs/prd.md) | Product requirements: actors, record shape, API, retention, and acceptance tests. |
| [docs/architecture.md](docs/architecture.md) | How version 1 runs: one application, with controller, service, and repository packages. |

Publishing is the client’s job. Version 1 does not ship a publisher SDK, does not watch databases, and does not invent a change a client never published. The owning application remains the system of record for the business object. The configured store is the system of record for stored history. PostgreSQL is the store that ships with the repository. Kafka is the buffer between the client commit and that store.

## What a host runs

A host operates three things:

1. A database. The default scripts create a PostgreSQL database named `activitytrail`.
2. A Kafka cluster with the topics in the architecture document.
3. This service. One process registers applications, reads history, and consumes Kafka.

Support engineers do not log into Activity Trail. A support application calls the read API with the application id in the path.

## Access control belongs to the host

Activity Trail is open source. A team clones the repository and hosts it for its own applications. Each host already has its own network policy, identity system, and Kafka access rules. Those differ from one organization to the next, so this service ships the history path and leaves caller authentication to the host.

The host applies that policy outside the process, with a private network, a gateway, or Kafka ACLs. A host whose policy must run inside the service extends this code and keeps the record rules, the topic, and the read response. The shipped service does not authenticate callers and does not decide who may use Kafka.

| Question | Answer in the shipped service |
| --- | --- |
| How does an application id get registered? | `POST /v1/operator/applications`. The body carries `application_id` and `display_name`. The response is 201. The same id again is 409. Events for an id that was never posted are not stored. |
| Who may call that POST, the read API, or the other HTTP routes? | The hosting organization. Put the API on a private network or behind a gateway. To enforce the check inside this process, extend the code. |
| Who may produce to `activitytrail.field-changes`? | The host’s Kafka ACLs. The consumer stores a message when its `application_id` is registered and enabled. |
| Who may run the consumer or read the dead-letter topic? | The host’s Kafka ACLs. The process joins group `activitytrail` when it starts. |

## Create the default database

Install PostgreSQL 16 or newer. From this repository:

```sh
psql -d postgres -v ON_ERROR_STOP=1 -f db/postgres/001_create_database.sql
psql -d activitytrail -1 -v ON_ERROR_STOP=1 -f db/postgres/V1__schema.sql
```

`001_create_database.sql` creates a database named `activitytrail`. It fails if that name already exists. `V1__schema.sql` creates `application`, `field_activity_trail`, and `retention_run`, the check constraints, and the default indexes. The schema script fails if those tables already exist.

The default `field_activity_trail` table is unpartitioned. The default indexes are:

- primary key on `event_id`
- object history on `(application_id, object_name, object_id, updated_at, recorded_at, event_id)`
- single-field history, the same key with `object_field` before `updated_at`
- retention on `(application_id, updated_at)`

When the JAR exists, Flyway applies `db/postgres/V1__schema.sql` on startup if the tables are not already there. A host who applied it by hand sets the Flyway baseline to version 1 so startup does not apply that script twice:

```sh
export SPRING_FLYWAY_BASELINE_ON_MIGRATE=true
export SPRING_FLYWAY_BASELINE_VERSION=1
```

## What a host can change

These changes stay outside the HTTP API and the Kafka contract.

| Setting | Default | How a host changes it |
| --- | --- | --- |
| Database | PostgreSQL, in the `repository` package | Replace `PostgresFieldActivityRepository`. The controller and the service stay as they are. `db/postgres` applies only to the PostgreSQL implementation. |
| Indexes and partitions | The indexes in `V1__schema.sql`. The table is unpartitioned. | Add SQL under [`db/postgres/custom`](db/postgres/custom) with Flyway versions `V1000` and above, for example `V1000__extra_index.sql`. The application issues the logical read and insert. It does not name secondary indexes. Keep `event_id` unique and keep the check constraints. |
| Retention window | 365 days for a new application | Set `RETENTION_DAYS_DEFAULT`, or send `retention_days` from 1 to 3650 on the register POST. |
| Retention deletion | Enabled | Set `RETENTION_ENABLED=false` to stop all deletion. Set an application’s `retention_days` to JSON `null` to keep that application’s rows while other applications still expire. |

`RETENTION_ENABLED=false` is the switch that turns deletion off for the whole installation. The daily job can stay scheduled. The pass exits successfully and deletes nothing. Leaving `retention_days` empty on one application turns deletion off for that application only.

A custom script may replace the three secondary indexes. On PostgreSQL, partitioning `field_activity_trail` by time means the primary key has to include the partition column. The shipped insert uses a conflict target of `event_id` alone, which matches the unpartitioned primary key. A partitioned layout that changes that target is a change in the store adapter, not in the read API.

## Environments

Shared settings are in `src/main/resources/application.properties`. Each environment has its own file. The process refuses to start until one of these profiles is selected.

| Profile | File | Database and Kafka |
| --- | --- | --- |
| `local` | `application-local.properties` | Local PostgreSQL and `localhost:9092`. |
| `qal` | `application-qal.properties` | `DATABASE_URL`, `KAFKA_BROKERS`, and `KAFKA_SASL_JAAS` from the environment. |
| `e2e` | `application-e2e.properties` | Same variables, pointed at the end-to-end cluster. |
| `prod` | `application-prod.properties` | Same variables, pointed at production. |

```sh
java -jar build/libs/activitytrail.jar --spring.profiles.active=prod
```

`DATABASE_URL` is the JDBC URL. `KAFKA_BROKERS` is the broker list. `KAFKA_SASL_JAAS` is the Kafka login for QAL, E2E, and prod. Those values stay in the environment, not in the profile files. A host that authenticates PostgreSQL separately can add `spring.datasource.username` and `spring.datasource.password` to the profile.

| Variable | Default | Purpose |
| --- | --- | --- |
| `RETENTION_ENABLED` | `true` | `false` disables every retention delete. |
| `RETENTION_DAYS_DEFAULT` | `365` | Window stored when registration omits `retention_days`. |

## Register an application

Call the operator API on the private network. Omit `retention_days` to store the deployment default of 365. Send `null` to keep that application’s history until a person deletes it outside the job.

```sh
curl -sS -X POST "http://localhost:8080/v1/operator/applications" \
  -H "Content-Type: application/json" \
  -d '{"application_id":"customer-profile","display_name":"Customer Profile"}'
```

The 201 response is the registry row. It does not include a secret. Register `orders` the same way. A second registration of the same `application_id` returns 409. Send `"retention_days": 90` to keep 90 days, or `"retention_days": null` to keep that application's rows.

## Publish a field change

After the publishing application commits its own transaction, it produces one JSON object per changed field to `activitytrail.field-changes`. The message key is `{application_id}:{object_name}:{object_id}`. Each application uses its own id in that key and in the body. The consumer stores the row under that `application_id` when the application is registered and enabled. Version 1 does not require a publish header.

```json
{
  "event_id": "6f1c2a10-7b44-4e2a-9c31-0a6d5e8b1001",
  "application_id": "customer-profile",
  "object_name": "userProfile",
  "object_id": "u-100",
  "object_field": "dateOfBirth",
  "change_type": "update",
  "old_value": "1990-04-12",
  "new_value": "1990-04-21",
  "updated_by": "user:88421",
  "updated_at": "2026-11-01T09:14:02Z"
}
```

The client’s request returns when its own database commits. It does not wait for Activity Trail. A change that is never published is absent from the read API. An empty history means no event was stored.

## Read history

The support application calls the path for that application:

```sh
curl -sS \
  "http://localhost:8080/v1/applications/customer-profile/objects/userProfile/u-100?field=dateOfBirth"
```

A matching history returns 200 and the old value, the new value, `updated_by`, `updated_at`, and `recorded_at`. No stored rows returns 200 and `"items": []`. The order trail is a different path, `.../applications/orders/objects/order/o-9`. Version 1 does not reject a caller for lacking a read key. Do not expose this API on a public network.

`GET /health` returns 200 when the database answers, and 503 when it does not.

## Retention

The same process deletes aged rows at 03:00 UTC. With `RETENTION_ENABLED=true`, the pass deletes rows whose `updated_at` is older than that application’s `retention_days`, 5000 rows at a time. Deletion is permanent. Applications with `retention_days` null are skipped. With `RETENTION_ENABLED=false`, the pass deletes nothing.

Personal data such as email and date of birth stays on the application’s read path until a retention pass deletes the row, or indefinitely when deletion is turned off. Version 1 does not encrypt field values. Database access is as sensitive as the trail.
