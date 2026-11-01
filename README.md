# Activity Trail

## What this repository is

Activity Trail is an open-source Java service a team clones and runs in its own environment. The repository name is `activitytrail`. Version 1 is one Java 21 process on Spring Boot 4.1.2, built with Gradle. The artifact is `build/libs/activitytrail.jar`.

Publishing applications send one Kafka event per changed field. This process consumes that topic, stores one PostgreSQL row per event, and serves the history through a read API. A support tool calls that API for one object in one application and receives who changed a field, when it changed, the old value, and the new value.

The publishing application remains the system of record for the business object. Activity Trail is the system of record for the history it has stored. Kafka is only the buffer between the client's commit and that store. The product rules are in [docs/prd.md](docs/prd.md). The code shape is in [docs/architecture.md](docs/architecture.md).

## What problem it solves

Software clients need a shared field-level history without building a separate audit store for every application.

A customer-profile service and an orders service can both publish to one topic. Support can then ask, for one profile or one order, which field changed, who changed it, the previous value, and the new value. The client's own request returns when its database commits. It does not wait for Activity Trail. A change that was never published is absent from the read API. An empty history means no event was stored. It does not mean the source data never changed.

Activity Trail does not watch databases, does not ship a publisher SDK, and does not invent a change a client never published.

## Clone, customize, and self-host

A host runs three things: PostgreSQL 16 or newer, a Kafka cluster, and this process.

```sh
git clone https://github.com/Amitercse/ActivityTrail.git
cd ActivityTrail
./gradlew bootJar
java -jar build/libs/activitytrail.jar --spring.profiles.active=local
```

Start the JAR from the repository root so Flyway can read `db/postgres`. The process listens on port 8080, applies the schema, and joins Kafka consumer group `activitytrail`. The profile must be `local`, `qal`, `e2e`, or `prod`. Without one of those, the process refuses to start.

| Profile | File | Database and Kafka |
| --- | --- | --- |
| `local` | `application-local.properties` | Local PostgreSQL and `localhost:9092`. |
| `qal` | `application-qal.properties` | `DATABASE_URL`, `KAFKA_BROKERS`, and `KAFKA_SASL_JAAS` from the environment. |
| `e2e` | `application-e2e.properties` | Same variables, pointed at the end-to-end cluster. |
| `prod` | `application-prod.properties` | Same variables, pointed at production. |

`DATABASE_URL` is the JDBC URL. `KAFKA_BROKERS` is the broker list. QAL, E2E, and prod do not commit hostnames or passwords. A host that authenticates PostgreSQL separately adds `spring.datasource.username` and `spring.datasource.password`.

Create the Kafka topics before the process starts:

| Topic | Partitions | Cleanup | Retention |
| --- | --- | --- | --- |
| `activitytrail.field-changes` | 12 | `delete` | 7 days (`retention.ms=604800000`). A host may raise this and must not lower it. |
| `activitytrail.field-changes.dlq` | 12 | `delete` | 14 days |

The database scripts are in [`db/postgres`](db/postgres). Flyway applies `V1__schema.sql` on startup when the tables are not already there. A host who applied that script by hand sets `SPRING_FLYWAY_BASELINE_ON_MIGRATE=true` and `SPRING_FLYWAY_BASELINE_VERSION=1`.

```sh
psql -d postgres -v ON_ERROR_STOP=1 -f db/postgres/001_create_database.sql
psql -d activitytrail -1 -v ON_ERROR_STOP=1 -f db/postgres/V1__schema.sql
```

A container image is `docker build -t activitytrail .` after the JAR exists. Pass the same Spring profile when the container starts.

These customizations stay outside the HTTP and Kafka contracts:

| What to change | How |
| --- | --- |
| Database | Replace `PostgresFieldActivityRepository` and the bean in `PostgresRepositoryConfiguration`. The controller and the service keep calling `FieldActivityRepository`. `db/postgres` applies only to PostgreSQL. |
| Indexes and partitions | Add SQL under [`db/postgres/custom`](db/postgres/custom) with Flyway versions `V1000` and above. Keep `event_id` unique and keep the check constraints. |
| Retention window | `RETENTION_DAYS_DEFAULT` (default 365), or `retention_days` from 1 to 3650 on registration. JSON `null` keeps that application's rows. |
| Retention deletion | `RETENTION_ENABLED=false` does not create the cleanup bean, so nothing is deleted. |
| Access checks inside the process | Extend the controller or the consumer. Keep the record rules, the topic, and the read response. |

## Authentication and access control

This service does not authenticate callers and does not ship API keys, passwords, or tokens.

That is deliberate. The project is cloned and self-hosted. Each organization already has its own identity system, network policy, and Kafka ACLs, and those policies differ. A login built into this process would replace those policies with one that does not fit every host.

The host decides:

| Door | What this service does | What the host controls |
| --- | --- | --- |
| Registration and read API | Accepts the HTTP calls below. It does not check a credential. | Which programs can reach port 8080, using a private network or a gateway. |
| Kafka produce | Stores a message when its `application_id` is registered and enabled and the key matches the body. | Which clients may write `activitytrail.field-changes`. |
| Kafka consume | Joins group `activitytrail` at startup. | Which principal may join that group, and which principal may write the dead-letter topic. |

A host that needs the check inside this process extends the code. Disabling an application is an update to the `application` row (`enabled = false`). There is no update route. A disabled application returns 403 on read, and new events for it are dead-lettered. Its stored rows remain until retention deletes them.

## Kafka and API contracts

Identifiers use `^[A-Za-z0-9._:-]{1,128}$`. `object_id` is 1–256 characters, not blank, and must not contain `/`. Values are UTF-8 text of at most 8192 characters. Empty string is a real value. Unknown JSON fields are ignored. The client does not send `body_hash` or `recorded_at`.

### Producer

After the publishing application commits its own transaction, it produces one JSON object per changed field to `activitytrail.field-changes`.

The message key is `{application_id}:{object_name}:{object_id}` and must match the body. There is no required header.

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
  "updated_at": "2026-11-01T09:14:02Z",
  "correlation_id": "save-7c2e91"
}
```

| Field | Rule |
| --- | --- |
| `event_id` | Client UUID. Same id and same body are stored once. Same id and a different body do not overwrite the row. |
| `application_id` | Registered application id. |
| `object_name`, `object_field`, `updated_by` | Identifier pattern. `updated_by` is required. The convention is `user:{id}` or `system:{name}`. |
| `object_id` | 1–256 characters, no `/`. |
| `change_type` | `set`, `update`, or `clear`. |
| `old_value` | Null only for `set`. |
| `new_value` | Null only for `clear`. An `update` requires both values, and they must differ. |
| `updated_at` | Client commit time, ISO-8601. More than five minutes ahead of the database clock is rejected. |
| `correlation_id` | Optional. Groups the fields of one save. |

`set` has a null old value and a non-null new value. `clear` has a non-null old value and a null new value.

### Consumer

This process is the consumer. It joins group `activitytrail` and reads `activitytrail.field-changes`. It commits the offset after the row is stored, after a duplicate is recognized, or after the dead letter is published. A database error is not committed. The listener retries that record.

| Dead-letter reason | When |
| --- | --- |
| `invalid_json` | The value is not JSON. |
| `validation_failed` | The JSON breaks the record rules, the key does not match the body, or `updated_at` is more than five minutes ahead. |
| `unknown_application` | `application_id` was never registered. |
| `application_disabled` | The application is registered and disabled. |
| `event_conflict` | `event_id` is already stored with a different body. |

The dead-letter topic is `activitytrail.field-changes.dlq`. The header is `X-Activity-Trail-Reason`. The body is:

```json
{
  "reason": "validation_failed",
  "failed_at": "2026-11-01T09:16:00.100000Z",
  "original": "{...exact message text...}"
}
```

`failed_at` is the PostgreSQL clock. The read API does not serve the dead-letter topic.

### Register an application

`POST /v1/operator/applications`

```json
{
  "application_id": "customer-profile",
  "display_name": "Customer Profile",
  "retention_days": 365
}
```

`application_id` is required and must match the identifier pattern. `display_name` is required, 1–200 characters, and not blank. `retention_days` is optional. Omit it to store `RETENTION_DAYS_DEFAULT`, which is 365 unless the host changed it. JSON `null` keeps that application's rows. A number must be an integer from 1 to 3650.

Success is 201:

```json
{
  "application_id": "customer-profile",
  "display_name": "Customer Profile",
  "retention_days": 365,
  "enabled": true
}
```

The same id again is 409 `application_exists`. An invalid body is 400 `validation_failed`. Until this POST succeeds, Kafka messages for that id are dead-lettered as `unknown_application` and are not stored. `enabled` starts as true. There is no list route and no update route.

```sh
curl -sS -X POST "http://localhost:8080/v1/operator/applications" \
  -H "Content-Type: application/json" \
  -d '{"application_id":"customer-profile","display_name":"Customer Profile"}'
```

### Read an audit trail

```
GET /v1/applications/{applicationId}/objects/{objectName}/{objectId}?field=&from=&to=&limit=&cursor=
```

The path selects one object. Query parameters are optional.

| Query | Rule |
| --- | --- |
| `field` | Exact field name. Omit it to return every stored field for that object. |
| `from` | Inclusive lower bound on `updated_at`. |
| `to` | Exclusive upper bound on `updated_at`. |
| `limit` | Page size. Default 50. Range 1–200. |
| `cursor` | Opaque token from the previous `next_cursor`. |

With no query parameters, the response is the first page of up to 50 rows for that object, newest first, with no time bound. Order is `updated_at`, then `recorded_at`, then `event_id`. The next page sends `next_cursor` back as `cursor`. Pages do not overlap. The API does not use `OFFSET`.

```sh
curl -sS \
  "http://localhost:8080/v1/applications/customer-profile/objects/userProfile/u-100?field=dateOfBirth"
```

Success is 200:

```json
{
  "application_id": "customer-profile",
  "object_name": "userProfile",
  "object_id": "u-100",
  "items": [
    {
      "event_id": "6f1c2a10-7b44-4e2a-9c31-0a6d5e8b1001",
      "object_field": "dateOfBirth",
      "change_type": "update",
      "old_value": "1990-04-12",
      "new_value": "1990-04-21",
      "updated_by": "user:88421",
      "updated_at": "2026-11-01T09:14:02.000000Z",
      "recorded_at": "2026-11-01T09:14:03.221412Z",
      "correlation_id": "save-7c2e91"
    }
  ],
  "next_cursor": null
}
```

`items` is empty when nothing is stored, including for an object the service has never seen. That is 200, not 404. `old_value`, `new_value`, and `correlation_id` are JSON strings or `null`. `next_cursor` is a string when another page exists and `null` on the last page. Timestamps are UTC. The response omits `body_hash`.

| Status | Code | When |
| --- | --- | --- |
| 200 | | History, including an empty `items` list. |
| 404 | `application_not_found` | The application was never registered. |
| 403 | `application_disabled` | The application is disabled. |
| 400 | `invalid_query` | The query is not valid. `from` equal to or later than `to` is this code. |
| 400 | `invalid_cursor` | The cursor is unreadable, or `field`, `from`, or `to` does not match it. |

A `/v1` error body is:

```json
{
  "error": {
    "code": "application_not_found",
    "message": "The application is not registered."
  }
}
```

`GET /health` returns 200 `{"status":"ok"}` when the database answers `SELECT 1`, and 503 `{"status":"not_ready"}` otherwise.

## Contributors

If you find a gap in the service, the docs, or the contracts above, open a pull request or an issue on https://github.com/Amitercse/ActivityTrail. The same place is where another team should ask for more information or for help running this service in their own environment. Describe the change you need, or the step that blocked you, and include the profile (`local`, `qal`, `e2e`, or `prod`) you are using. Keep the record rules, the topic, and the read response intact unless the proposal says why they should change.
