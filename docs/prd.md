# Activity Trail product requirements

Version 1. This document is the build contract. The decisions in it are locked. [Architecture](architecture.md) describes how this version runs and uses the same behavior, names, and acceptance tests.

The service is the Gradle project in this repository. This document is the behavior that project implements.

## 1. Product

Activity Trail is one shared service for the organization. Many applications publish field-level change events to one Kafka topic, `activitytrail.field-changes`. The service stores one row per changed field in PostgreSQL and serves that history through a read API.

A support tool calls the read API for one object in one application. The answer for each stored field change is:

- who changed the field
- when it changed
- the old value
- the new value

A stored row also carries when Activity Trail recorded it, so support can tell a client’s commit time apart from the time the history became readable.

The publishing application remains the system of record for the business object. Activity Trail is the system of record for the history it has stored. Kafka is only the buffer between the client’s commit and that stored history. The topic uses delete retention. It is not compacted, and the read API does not query it.

The service stores a change only when a client publishes it. It does not watch databases, run triggers, or reconstruct a value the client never sent. An empty history means no event was stored for that query. It does not prove the data never changed. Support tools must present an empty list as “no stored history,” and must not present it as proof that the field stayed the same.

Publishing is the client’s job. Clients may use their own code. Version 1 does not ship a publisher SDK.

One save that changes two fields is two events. They share a `correlation_id` and have different `event_id` values. History is stored per field, so a later reader can ask for one field or for every field of one object.

Version 1 is one Java process on Spring Boot, built with Gradle. The artifact is `build/libs/activitytrail.jar`. That process serves the register POST and the history GET, and the Kafka consumer starts when the process starts. The code is one application with controller, service, and repository packages. PostgreSQL lives in the repository package. Another database replaces that implementation. The service does not use Go. Layout is specified in the architecture.

Activity Trail is open source. A host clones this repository and runs it in their own environment. The host supplies Kafka and a database, then runs the service roles. There is no shared Activity Trail cloud that clients must call.

## 2. Actors

| Actor | Responsibility |
| --- | --- |
| Publishing application | After its own transaction commits, emits one event per changed field. Declares the actor in `updated_by`. Puts its registered `application_id` in the message body and the message key. |
| System actor | An automated actor named by the publisher, for example `system:fulfillment`. It is a value of `updated_by`, not a login to Activity Trail. |
| Support application | Calls the read API for one application id and one object. Shows stored history to support engineers. |
| Support engineer | Uses the support application. Does not log into Activity Trail and does not hold an Activity Trail account. |
| Platform operator | The person or system at the hosting organization that registers applications and sets retention and the enabled flag. Activity Trail does not authenticate this caller. |
| Activity Trail | Consumes the topic, stores rows, deletes by retention, and serves the read API. |

A person and an automated system are both valid actors. The publisher declares which one performed the change. Activity Trail stores that declaration and does not resolve it against a directory.

## 3. Business requirements

| ID | Requirement |
| --- | --- |
| BR-1 | One Activity Trail serves every application in the organization. |
| BR-2 | For each stored field change, record who changed it, when, the previous value, and the new value. |
| BR-3 | The actor may be a person or an automated system. The publisher declares the actor. |
| BR-4 | Support can retrieve the history of one object, such as one user profile or one order. |
| BR-5 | History is stored per field. One changed field is one event and one row. |
| BR-6 | Many applications publish to the same topic. Stored rows stay separated by `application_id`. |
| BR-7 | A read returns the trail named by the application id in the path. Who may call that path is the host’s responsibility. |
| BR-8 | A change that was not published is absent. The API must not present that gap as proof that nothing changed. |
| BR-9 | Email, date of birth, and similar values stay in the store until retention deletes them. A host who turns deletion off keeps those values until they remove them. Who may call the read API, publish to Kafka, or run the consumer is the host’s responsibility. |
| BR-10 | The client’s business commit does not wait for Activity Trail. Kafka is the buffer. |
| BR-11 | A host can run the service from a clone of this repository in their own environment. |
| BR-12 | PostgreSQL is the default store. A host can use another relational database or a document database by adding a store implementation. The HTTP API, the Kafka contract, and validation stay the same. |
| BR-13 | The repository ships a default unpartitioned table and the default indexes. A host can change secondary indexes and partitioning without changing the read API. |
| BR-14 | Retention defaults to 365 days. A host can choose another window, skip deletion for one application, or turn deletion off for the whole installation. |

These requirements are satisfied only by events a client actually publishes. Activity Trail does not backfill history for data that already existed before a publisher started emitting events.

## 4. Scope

Version 1 includes:

- an operator API to register applications, set retention, and enable or disable an application
- a consumer that reads `application_id` from the Kafka message and stores one row per accepted field change
- a dead-letter topic for rejected messages
- a read API for one object in one application
- a daily retention job that permanently deletes aged rows when deletion is enabled
- a PostgreSQL schema script a host can apply before the service JAR exists
- a store boundary so another database is a new store implementation rather than a change to the API
- process health and readiness endpoints that return no trail data

Version 1 does not include:

- caller authentication inside Activity Trail
- Kafka ACLs, and any other control of who may produce to the topic or join the consumer group
- a publisher SDK
- a support user interface
- end-user accounts
- cross-application search, or any search that lists objects
- Debezium, database triggers, or any other capture from a source database
- a hash-chained ledger
- field encryption
- Schema Registry, or a per-field type registry inside Activity Trail
- querying Kafka as the trail

The caller of the read API already knows the application, the object name, and the object id. Activity Trail does not offer a way to discover objects.

## 5. Use cases

### UC-1. A person changes a date of birth

A support agent, or the customer, corrects a date of birth in the customer-profile application. That application is the system of record for the profile. It updates its own database and commits. The user-facing request succeeds on that commit.

After the commit, customer-profile publishes one event for the field `dateOfBirth` on object `userProfile` `u-100`:

| Field | Value |
| --- | --- |
| `event_id` | `6f1c2a10-7b44-4e2a-9c31-0a6d5e8b1001` |
| `application_id` | `customer-profile` |
| `object_name` | `userProfile` |
| `object_id` | `u-100` |
| `object_field` | `dateOfBirth` |
| `change_type` | `update` |
| `old_value` | `1990-04-12` |
| `new_value` | `1990-04-21` |
| `updated_by` | `user:88421` |
| `updated_at` | `2026-11-01T09:14:02Z` |

`updated_by` names the person. `updated_at` is when customer-profile committed, in UTC. The message key is `customer-profile:userProfile:u-100`.

Activity Trail later stores one row. It sets `recorded_at` from the PostgreSQL clock. Support can then read that row and see the person, the client commit time, the store time, `1990-04-12`, and `1990-04-21`.

The service does not check the profile table to confirm the date. If customer-profile publishes a wrong old value, that wrong value is what support sees.

### UC-2. Several fields in one save

A person saves a profile form that changes email and date of birth together. customer-profile commits both columns in one business transaction, then publishes two events. The events use different `event_id` values and the same `correlation_id`, `save-7c2e91`. They name the same `userProfile` `u-100`, the same `updated_by`, and the same `updated_at`.

| `event_id` | `object_field` | `old_value` | `new_value` |
| --- | --- | --- | --- |
| `6f1c2a10-7b44-4e2a-9c31-0a6d5e8b1001` | `dateOfBirth` | `1990-04-12` | `1990-04-21` |
| `6f1c2a10-7b44-4e2a-9c31-0a6d5e8b1002` | `email` | `a@example.com` | `b@example.com` |

Both messages use the Kafka key `customer-profile:userProfile:u-100`, so they stay in one partition. Activity Trail stores two rows. A read of `u-100` without a field filter returns both. A read with `field=dateOfBirth` returns only the date row. The shared `correlation_id` tells support the fields were saved together. The service does not collapse them into one row.

### UC-3. A system changes order status from placed to shipped

The fulfillment system ships order `o-9`. The orders application commits the new status, then publishes:

| Field | Value |
| --- | --- |
| `event_id` | `91aa0c33-1d20-4f77-8b10-55c0e1d20009` |
| `application_id` | `orders` |
| `object_name` | `order` |
| `object_id` | `o-9` |
| `object_field` | `status` |
| `change_type` | `update` |
| `old_value` | `placed` |
| `new_value` | `shipped` |
| `updated_by` | `system:fulfillment` |
| `updated_at` | `2026-11-01T09:15:00Z` |

The actor is the fulfillment system, named by the orders application. Activity Trail does not require a person. This event goes to the same topic as the profile events. The message key is `orders:order:o-9`. The stored row is separated by `application_id` `orders`. A read of the customer-profile path does not return it.

### UC-4. Set and clear

The first time a profile gains an email, there is no previous value. customer-profile publishes `change_type` `set` with `old_value` null and `new_value` `a@example.com`.

Later the email is removed. customer-profile publishes `change_type` `clear` with `old_value` `a@example.com` and `new_value` null.

Both events are stored. They are two rows in the history of that field.

An `update` whose old and new values are the same is not a change. The consumer dead-letters it and stores nothing. Empty string is a real value: a set to `""`, a clear from `""`, and an update from `""` to `"x"` are valid. An update from `""` to `""` is rejected.

Activity Trail does not compare the new event with the previous stored row. Two `set` events in a row are both stored if each event is valid on its own. The publisher owns the claim about previous state.

### UC-5. Support lookup

A support engineer opens user `u-100` in the support tool. The engineer authenticates to that tool. The tool calls:

`GET /v1/applications/customer-profile/objects/userProfile/u-100?field=dateOfBirth`

Activity Trail reads the store for that application id and object. The tool shows `updated_by`, `updated_at`, `old_value`, `new_value`, and `recorded_at` from the stored rows. The engineer never receives an Activity Trail login, and the tool does not query Kafka. Version 1 does not check a credential on this call. The host decides which programs can reach the API.

If no row has been stored, the API returns 200 and an empty list. The tool says that no history is stored. It does not say the date of birth never changed. `recorded_at` can be later than `updated_at`, because the business commit and the consume step are separate. The tool can show both times.

A read without `field` returns every stored field for that profile, and no other object. The orders application is invisible on this path.

### UC-6. Many applications

The organization runs one Activity Trail. customer-profile and orders both publish to `activitytrail.field-changes`. Each is a registered `application_id`. Each publish uses its own Kafka message key.

A profile event uses the key `customer-profile:userProfile:u-100` and a body whose `application_id` is `customer-profile`. An order event uses the key `orders:order:o-9` and a body whose `application_id` is `orders`. The consumer stores each row under the `application_id` in that message. A read of the customer-profile path returns profile rows. A read of the orders path returns order rows. The two trails stay apart because the ids differ, not because of a credential.

Disabling an application stops new reads and stops new stores for that application. It does not merge or expose another application’s rows.

### UC-7. Invalid events

A publisher can emit a message the service must not store. The consumer acknowledges the Kafka record only after the rejection is durable on the dead-letter topic. The row is not inserted, so a later read does not show it.

The rejection reasons are:

| Reason | When |
| --- | --- |
| `invalid_json` | The message value is not JSON. |
| `validation_failed` | The JSON breaks the record rules, including a missing `updated_by`, an `update` with equal values, or an `updated_at` more than five minutes ahead of the database clock. |
| `unknown_application` | `application_id` is not registered. |
| `application_disabled` | The application is registered and disabled. |
| `event_conflict` | `event_id` is already stored with a different canonical body. |

A missing `updated_by` is `validation_failed`. That event is not readable. A valid event for a disabled application is `application_disabled` and is not readable until a later, distinct, successful publish happens after the application is enabled again. The dead-lettered copy itself is never served by the API.

### UC-8. Duplicate delivery

Kafka delivery is at least once. A client retry and a consumer crash can both deliver the same event twice.

If the second delivery has the same `event_id` and the same canonical body, Activity Trail acknowledges it and leaves the original row unchanged, including `recorded_at`. If the second delivery has the same `event_id` and a different canonical body, Activity Trail leaves the first row unchanged and dead-letters the second with `event_conflict`.

A second consumer in the same consumer group does not create a second row. Partition assignment gives each record to one member, and a redelivered record meets the same `event_id` rule.

### UC-9. Retention

Each application has a retention period. A new application stores 365 days unless the host set `RETENTION_DAYS_DEFAULT` to another value, or the registration body sent a different `retention_days`. The operator can set a window from 1 to 3650 days. The operator can set `retention_days` to null, which keeps that application’s rows. `RETENTION_ENABLED=false` turns deletion off for every application in that installation. The daily job still runs and deletes nothing.

When deletion is enabled, Activity Trail deletes rows whose `updated_at` is older than that application’s window, once a day at 03:00 UTC. Deletion is permanent. Deleted values are not readable, not archived by the service, and not reconstructed. Applications with a null window are left in place.

Email and date of birth live in the row for an authorized reader until that delete. When the host has turned deletion off, they remain stored. After a delete, a read returns the remaining rows, or an empty list when none remain. The empty list still means no stored history.

A late event whose `updated_at` is already outside an active window is stored if it passes validation, and the next retention run deletes it. Publishers that need a correction to remain visible must publish an `updated_at` inside the window, lengthen retention, or turn deletion off before the job runs.

### UC-10. The client commit stays independent of this service

customer-profile commits the new date of birth in its own database and returns success to its caller. That transaction does not call Activity Trail, does not wait for a stored row in Activity Trail, and does not wait for the consumer.

After the business commit, the publishing application produces to Kafka. Kafka holds the record for at least seven days, which absorbs a stopped or slow consumer. When the consumer is down, reads of history already stored keep working. Events produced while it is down are stored once after it restarts, as long as they are still within topic retention.

If the produce itself fails after the business commit, no event is stored. The profile still shows the new date of birth. The trail does not. That gap is a missing publish. Activity Trail does not notice it and does not call the source application to repair it. Repair is a new publish by the client, with its own `event_id`.

## 6. Record

One stored record is one changed field.

### 6.1 Client fields

| Field | Rule |
| --- | --- |
| `event_id` | Client UUID. Unique. Identifies the event for retries. Any RFC 4122 textual UUID. Compared case-insensitively and stored lowercase. |
| `application_id` | Registered id, for example `customer-profile`. Pattern `^[A-Za-z0-9._:-]{1,128}$`. Case-sensitive. |
| `object_name` | For example `userProfile` or `order`. Same pattern as `application_id`. |
| `object_id` | Id in the publishing application. 1 to 256 characters, not blank, and it must not contain `/`. Not blank means the value contains a non-whitespace character. |
| `object_field` | For example `dateOfBirth`, `email`, or `status`. Same pattern as `application_id`. |
| `change_type` | `set`, `update`, or `clear`. Names a first value, a replacement, or a removal. The database check uses it to require the matching nulls. |
| `old_value` | UTF-8 text, or null. Null only for `set`. At most 8192 characters. |
| `new_value` | UTF-8 text, or null. Null only for `clear`. At most 8192 characters. |
| `updated_by` | Required. Same pattern as `application_id`. Convention `user:{id}` or `system:{name}`. The service checks the pattern and does not check the prefix. |
| `updated_at` | When the client committed the change. ISO-8601 instant, expressed with `Z` or a numeric offset. Stored in UTC. More than five minutes ahead of the PostgreSQL clock is rejected. Any earlier timestamp is allowed. |
| `correlation_id` | Optional. When present, the same pattern as `application_id`. Groups fields saved together. Omitted and null mean the same thing. |

`set` requires a null old value and a non-null new value. `update` requires both values, and they must differ. `clear` requires a non-null old value and a null new value. Empty string is a real value and satisfies “non-null.”

Values are UTF-8 text. Dates are `YYYY-MM-DD` or ISO-8601 text. Booleans are `true` or `false`. Numbers are decimal text. The service does not coerce types and does not normalize numbers or dates inside the value. `"10"` and `"10.0"` are different texts. JSON numbers, JSON booleans, objects, and arrays are rejected. The encoding rules are a publisher obligation so that support can read the text. Version 1 has no per-field schema and does not reject a date-shaped field that contains some other string.

`object_id` is a single URL path segment on read. Publishers percent-encode reserved characters other than `/`. A value that contains `/` is `validation_failed`.

### 6.2 Server fields

| Field | Rule |
| --- | --- |
| `recorded_at` | When Activity Trail stored the row. Assigned by PostgreSQL in UTC. Clients do not send it. A client field of this name is ignored. |
| `body_hash` | SHA-256 of the canonical validated body, defined below. Stored so a redelivered event with the same body is recognized, and so a reused `event_id` with a different body cannot overwrite the row. Clients do not send it. A client field of this name is ignored. Not returned by the read API. |

### 6.3 Canonical body and hash

Unknown JSON fields are ignored before validation and before the body hash. The known client fields are exactly the names in section 6.1. `recorded_at`, `body_hash`, and any other key are unknown.

The hash is computed only after validation succeeds. The canonical body is UTF-8 JSON with no insignificant whitespace, with RFC 8259 string escaping, and without HTML escaping. Keys appear in this order:

1. `event_id`
2. `application_id`
3. `object_name`
4. `object_id`
5. `object_field`
6. `change_type`
7. `old_value`
8. `new_value`
9. `updated_by`
10. `updated_at`
11. `correlation_id`

`event_id` is lowercase. `updated_at` is UTC with exactly six fractional digits, for example `2026-11-01T09:14:02.000000Z`. Nulls are JSON `null`, including a missing `correlation_id` and a missing value that the change type requires to be null. `body_hash` is the SHA-256 of those bytes, stored as 32 bytes.

Two payloads that differ only by unknown fields, by UUID letter case, or by an equivalent offset such as `Z` and `+00:00`, have the same hash. A different `new_value`, `correlation_id`, or any other canonical field has a different hash.

### 6.4 Prohibited content

Publishers must not publish passwords, session tokens, access keys, card numbers, or government-id images.

That is a publisher obligation. Version 1 does not classify message contents. A publisher that breaks the rule can cause the value to be stored until retention deletes it, and to be copied onto the dead-letter topic if the message is rejected for another reason. Operators must not treat the store as a scanner.

Values are capped at 8192 characters. That limit bounds a row. It is not a content filter.

### 6.5 Worked payloads

Date of birth update:

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

The email event from the same save is the same envelope with `event_id` `6f1c2a10-7b44-4e2a-9c31-0a6d5e8b1002`, `object_field` `email`, `old_value` `a@example.com`, and `new_value` `b@example.com`.

Order status:

```json
{
  "event_id": "91aa0c33-1d20-4f77-8b10-55c0e1d20009",
  "application_id": "orders",
  "object_name": "order",
  "object_id": "o-9",
  "object_field": "status",
  "change_type": "update",
  "old_value": "placed",
  "new_value": "shipped",
  "updated_by": "system:fulfillment",
  "updated_at": "2026-11-01T09:15:00Z"
}
```

Set:

```json
{
  "event_id": "c0ffee00-0000-4000-8000-0000000000a1",
  "application_id": "customer-profile",
  "object_name": "userProfile",
  "object_id": "u-100",
  "object_field": "email",
  "change_type": "set",
  "old_value": null,
  "new_value": "a@example.com",
  "updated_by": "user:88421",
  "updated_at": "2026-10-01T12:00:00Z"
}
```

Clear:

```json
{
  "event_id": "c0ffee00-0000-4000-8000-0000000000a2",
  "application_id": "customer-profile",
  "object_name": "userProfile",
  "object_id": "u-100",
  "object_field": "email",
  "change_type": "clear",
  "old_value": "a@example.com",
  "new_value": null,
  "updated_by": "user:88421",
  "updated_at": "2026-10-02T12:00:00Z"
}
```

## 7. Kafka

| Item | Value |
| --- | --- |
| Topic | `activitytrail.field-changes` |
| Dead letter | `activitytrail.field-changes.dlq` |
| Consumer group | `activitytrail` |
| Message key | `{application_id}:{object_name}:{object_id}` |
| Cleanup | `delete`. The topic is not compacted. |
| Main topic retention | At least 7 days. The deployed setting is 7 days. Operators may raise it and must not lower it. |
| Main topic partitions | 12 |
| Dead-letter retention | 14 days |

One host can run many applications on this topic. Each application sends its own message keys. The key for a field change is `{application_id}:{object_name}:{object_id}`. The body carries the same `application_id`. The consumer loads that id, and stores the row only when the application is registered and enabled.

The message key must equal `{application_id}:{object_name}:{object_id}` taken from the validated body. A mismatch is `validation_failed`. The key keeps one object’s fields in one partition. Correctness of a stored row does not depend on Kafka order. Reads sort by the timestamps and `event_id` in section 9.

The host decides which principals may produce to `activitytrail.field-changes`, which principal may join the consumer group `activitytrail`, and which principal may write the dead-letter topic. Activity Trail does not configure or check those ACLs. A host who wants the service itself to reject a producer extends the consumer. The process starts with profile `local`, `qal`, `e2e`, or `prod`.

Dead-letter body:

```json
{
  "reason": "validation_failed",
  "failed_at": "2026-11-01T09:16:00.100000Z",
  "original": "{...exact message text...}"
}
```

`reason` is one of the five reasons in UC-7. `failed_at` is the PostgreSQL clock in UTC when the rejection was decided. `original` is the exact message text. It may contain personal data. The dead-letter topic is not exposed through the API.

The same `event_id` and the same canonical body are acknowledged and stored once. The same `event_id` and a different body are not overwritten and are dead-lettered as `event_conflict`. The consumer commits the Kafka offset only after the PostgreSQL commit, or after the dead-letter publish succeeds. A crash may redeliver. Redelivery is safe.

Deterministic rejections use the dead-letter path. PostgreSQL or dead-letter outages are not rejections: the consumer leaves the offset uncommitted and retries.

## 8. Application identity and access control

Version 1 tells applications apart by `application_id`. The publisher puts that id in the Kafka message body and in the message key. The consumer stores the row under that id. A host with several applications sends several keys, one per object, each beginning with that application’s id.

### 8.1 How an application id is registered

Activity Trail exposes an HTTP endpoint for registration. The host calls:

`POST /v1/operator/applications`

with `application_id`, `display_name`, and optional `retention_days`. A new id returns 201 and inserts one `application` row. The same id again returns 409. That POST is the only registration route. A later change to `retention_days` or `enabled` is an update to the `application` row in the database.

Until that POST has succeeded, a Kafka message for the id is dead-lettered as `unknown_application` and is not stored. A disabled id is `application_disabled`. There is no registry cache. A disable or a retention change applies on the next request and the next message.

The row holds `application_id`, `display_name`, `retention_days`, and `enabled`. It does not hold secrets.

### 8.2 Who may connect

Authentication, and the decision of who may publish or consume, belong to the organization hosting the service.

Activity Trail is open source and is run by the host, in the host’s environment. There is no shared Activity Trail cloud and no single login that every deployment must use. A host already chooses who may reach its network, who may call its internal APIs, and who may produce or consume on its Kafka cluster. Those policies are the host’s, and they are different in each organization. Building one credential scheme into this service would replace those policies with a scheme the host did not choose.

Version 1 therefore stores and serves history, and registers application ids, without checking a caller. The host keeps its own mechanism. When that mechanism belongs in this process, the host extends the code. The record shape, the topic contract, and the read response stay as specified in this document.

| Door | What the shipped service does | What the host controls |
| --- | --- | --- |
| Kafka produce | Reads `application_id` from the message and stores it when that id is registered and enabled. | Which clients may write `activitytrail.field-changes`. |
| Kafka consume | The process joins group `activitytrail` when it starts. | Which principal may join that group, and which principal may write the dead-letter topic. |
| Read API | Returns the trail for the application id in the path. | Which programs may call `GET /v1/...`. |
| Registration API | Accepts `POST /v1/operator/applications`. | Which programs may call that route. |

The shipped routes do not check a caller credential. A host limits them with network policy, a gateway, or Kafka ACLs. A host who wants the check inside this service extends the code and keeps the record rules, the topic, and the read response the same. The read API uses the `applicationId` in the path. A disabled application returns 403 `application_disabled`. An application id that is not registered returns 404 `application_not_found`. A registered, enabled application with no rows returns 200 and an empty list.

## 9. HTTP API

Base path `/v1`. Successful and error bodies are JSON, `Content-Type: application/json; charset=utf-8`.

Error envelope for `/v1`:

```json
{
  "error": {
    "code": "application_disabled",
    "message": "This application is disabled."
  }
}
```

| Status | `code` | Message |
| --- | --- | --- |
| 403 | `application_disabled` | This application is disabled. |
| 400 | `validation_failed` | The request is not valid. The message may name the field and the rule. |
| 400 | `invalid_query` | The query is not valid. |
| 400 | `invalid_cursor` | The cursor is not valid. |
| 404 | `application_not_found` | The application is not registered. |
| 409 | `application_exists` | The application is already registered. |

Health routes are outside `/v1` and return no trail data, no application ids, and no dependency connection strings.

| Route | 200 means |
| --- | --- |
| `GET /health` | The database answers `SELECT 1`. |

A failing check returns 503 and `{"status":"not_ready"}`. A passing check returns 200 and `{"status":"ok"}`.

### 9.1 Register an application

This route registers an application id. It does not authenticate the caller. The host decides who may call it, and may extend the service if the check should live in this process.

`POST /v1/operator/applications`

Body: `application_id`, `display_name`, and optional `retention_days`. Omitted `retention_days` stores `RETENTION_DAYS_DEFAULT`, which is 365 unless the host changed it. A JSON number must be an integer from 1 to 3650. A JSON `null` stores null and disables deletion for that application. `display_name` is required, from 1 to 200 characters, and not blank. `enabled` starts as true.

A duplicate `application_id` returns 409. Success returns 201:

```json
{
  "application_id": "customer-profile",
  "display_name": "Customer Profile",
  "retention_days": 365,
  "enabled": true
}
```

There is no list route and no update route. To change `retention_days` or set `enabled` to false, update the `application` row. A disabled application still exists. Publishes for it stay dead-lettered until `enabled` is true again. A read for it returns 403.

### 9.2 Read

```
GET /v1/applications/{applicationId}/objects/{objectName}/{objectId}?field=&from=&to=&limit=&cursor=
```

| Query | Rule |
| --- | --- |
| `field` | Optional exact `object_field`. |
| `from` | Optional RFC 3339 instant, inclusive lower bound on `updated_at`. |
| `to` | Optional RFC 3339 instant, exclusive upper bound on `updated_at`. |
| `limit` | Optional. Default 50. Maximum 200. Values outside 1 through 200 are 400. |
| `cursor` | Optional opaque keyset cursor. |

`from` later than `to`, or `from` equal to `to`, is 400 `invalid_query`. A date without a time is 400. The caller percent-encodes `objectId`. After decoding it must match the stored `object_id` rules.

Order is `updated_at` descending, then `recorded_at` descending, then `event_id` descending. The cursor is a keyset on that order. The API does not use `OFFSET`.

The cursor is opaque. Clients send `next_cursor` back as `cursor` and do not parse it. Filters are bound into the cursor. A later page may omit `field`, `from`, and `to`. If the client sends them, they must match the cursor. A mismatched or unreadable cursor is 400 `invalid_cursor`. Changing the filter means a new query without a cursor.

Empty history is 200 and an empty list, never 404. That includes an object the service has never seen. 404 is reserved for operator application routes.

Success body:

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

`items` is an array, empty when nothing is stored. `next_cursor` is a string when another page exists and null on the last page. `old_value` and `new_value` are JSON strings or JSON null, copied from storage. `correlation_id` is a string or null. Timestamps are UTC. The response omits `body_hash`.

The service builds a page by reading one extra row past `limit`. If that row exists, `next_cursor` is taken from the last row actually returned. The next request asks for rows strictly after that position in the sort. Pages do not overlap and do not skip a row that still matches the filter.

The path selects the application before the query. A disabled application is 403 whether or not rows exist. An unknown application id is 404. Rows remain stored while the application is disabled and become readable again if it is enabled before retention deletes them.

## 10. Retention and personal data

| Rule | Value |
| --- | --- |
| Default window | 365 days, from `RETENTION_DAYS_DEFAULT` |
| Minimum window | 1 day, when a window is set |
| Maximum window | 3650 days, when a window is set |
| Per application off | `retention_days` null. That application’s rows are kept. |
| Installation off | `RETENTION_ENABLED=false`. No application’s rows are deleted. |
| Clock | The store’s clock in UTC. One day is 24 hours. The PostgreSQL store uses `clock_timestamp()`. |
| Eligible row | Deletion is enabled, the application has a window, and `updated_at` is strictly older than that window |
| Schedule | Once a day at 03:00 UTC |
| Batch | 5000 rows |
| Result | Permanent delete |

Email, date of birth, and the other stored values are returned by a read of that application’s path. They remain until this job deletes the row. When deletion is off, they remain stored. Database administrators can also read the table. Version 1 does not encrypt fields. The host decides who can reach the API and the database. The service does not log values. Retention, when the host leaves it on, deletes aged rows.

The dead-letter topic can contain the same personal data for up to 14 days. Who may read that topic is the host’s Kafka ACL. The HTTP API does not read it.

Disabling an application does not delete rows. While deletion is enabled, retention still deletes aged rows for a disabled application that has a retention window.

## 11. Logging

Logs and metrics must not contain `old_value`, `new_value`, or `updated_by`.

Logs may contain `event_id`, `application_id`, `object_name`, `object_id`, `object_field`, `change_type`, `correlation_id`, `body_hash` as hex, Kafka partition and offset, dead-letter reason, HTTP status, and latency. The raw message body is not logged. A date-of-birth flow must not write the old or new date to the log.

## 12. What the service guarantees

- A stored row is one valid published field change, tied to the registered `application_id` in that Kafka message.
- Retries of the same canonical event create one row.
- A conflicting reuse of `event_id` does not change the stored row.
- Reads come from the configured store. The default store is PostgreSQL. A stopped consumer does not stop reads of rows already stored.
- The API and the consumer do not call each other. Both use the store. There is no registry cache.
- A host can replace the store, the secondary indexes, and the retention window without changing the read or publish contracts. `RETENTION_ENABLED=false` deletes nothing.
- The read API never describes an empty list as proof that the source data was unchanged.
- The service never invents a field change to fill a gap, including a gap caused by a failed produce after the client commit.

## 13. Acceptance tests

These tests are the version 1 acceptance set. The architecture maps each one to the mechanism that makes it pass. Concrete ids below are the contract fixtures.

### AT-1. Register two applications

Register `customer-profile` and `orders`. Each response is 201 and returns `application_id`, `display_name`, `retention_days`, and `enabled`. A second registration of `customer-profile` returns 409. The response contains no field values.

### AT-2. Two applications, one topic

Publish the date-of-birth update for `userProfile` `u-100` and the status update for `order` `o-9` to `activitytrail.field-changes`. The profile message key is `customer-profile:userProfile:u-100`. The order message key is `orders:order:o-9`. Each body carries its own `application_id`. After the consumer stores them, PostgreSQL has one customer-profile row and one orders row.

### AT-3. Support reads the date of birth

`GET /v1/applications/customer-profile/objects/userProfile/u-100?field=dateOfBirth` returns 200. The item includes `old_value` `1990-04-12`, `new_value` `1990-04-21`, `updated_by` `user:88421`, `updated_at` `2026-11-01T09:14:02Z` as the same instant, and a `recorded_at` assigned by the service. The request has no credential.

### AT-4. Each application id stays on its own path

`GET /v1/applications/customer-profile/objects/userProfile/u-100` does not return the order status row. `GET /v1/applications/orders/objects/order/o-9` returns the status row and does not return the date-of-birth row.

### AT-5. An object read stays on that object

Publish a second profile field for `u-100` and a field for `userProfile` `u-200`. `GET` `u-100` without `field` returns only rows whose object is `userProfile` `u-100`.

### AT-6. One save, two rows

Publish the email event and the date-of-birth event from UC-2, sharing `correlation_id` `save-7c2e91` and using different `event_id` values. A read of `u-100` returns two rows with that `correlation_id`.

### AT-7. Set, clear, and a no-op update

Publish the set and the clear from section 6.5. Both are stored. Publish an `update` whose `old_value` and `new_value` are both `placed`. That message is on the dead-letter topic with `validation_failed`, and no row exists for its `event_id`.

### AT-8. Missing actor

Publish a body that omits `updated_by`. The message is dead-lettered with `validation_failed`. A read of that object does not contain an item for that `event_id`.

### AT-9. Same event twice

Publish the date-of-birth event, then publish the same canonical body again with the same `event_id`. The table contains one row. `recorded_at` and `new_value` stay as they were after the first store.

### AT-10. Same event, different body

Store the date-of-birth event. Publish the same `event_id` with `new_value` `1991-01-01`. The stored `new_value` remains `1990-04-21`. The second message is dead-lettered with `event_conflict`.

### AT-11. A stopped consumer does not stop reads

Store one row. Stop the consumer process. `GET` that object still returns 200 and the stored row. Publish one new valid event while the consumer is stopped. Start the consumer. That event is stored once. The API process stayed up for the whole test.

### AT-12. Retention of one day

Set `customer-profile` retention to 1 day. Leave `RETENTION_ENABLED=true`. Store a row whose `updated_at` is more than one day before the store clock, and a row whose `updated_at` is inside the day. Run the retention job. The aged row is gone. The recent row remains. A read does not return the deleted value.

### AT-16. Retention deletion turned off

Set `RETENTION_ENABLED=false`. Store a row whose `updated_at` is older than the application’s window. Run the retention job. The row remains and a read still returns it. The process exits successfully.

### AT-17. One application kept indefinitely

Set `RETENTION_ENABLED=true`. Set `customer-profile` `retention_days` to null and `orders` `retention_days` to 1. Store an aged row for each application. Run the retention job. The customer-profile row remains. The aged orders row is gone.

### AT-13. Two members, one group

Run two consumers in group `activitytrail`. Publish one new event. The table contains one row for that `event_id`.

### AT-14. Logs omit the date

Run the date-of-birth publish and the read in AT-3. The logs of those steps do not contain `1990-04-12`, `1990-04-21`, or `user:88421`.

### AT-15. The message key and the body name the same application

Publish a body whose `application_id` is `customer-profile` with the message key `orders:userProfile:u-100`. The message is dead-lettered with `validation_failed`, and no row is stored. Publish the same body with the message key `customer-profile:userProfile:u-100` and a new `event_id`. The row is stored under `customer-profile`.

## 14. Glossary

| Term | Meaning |
| --- | --- |
| Trail | The stored field changes of one application. |
| Field activity | One changed field. One event. One row in `field_activity_trail`. |
| Change type | `set`, `update`, or `clear`. |
| Body hash | SHA-256 of the canonical event body, stored on the row and omitted from the read API. |
| Message key | Kafka key `{application_id}:{object_name}:{object_id}`. Each application sends its own keys. |
| Recorded at | When the store saved the row. The PostgreSQL store assigns it with `clock_timestamp()`. |
| Updated at | When the publishing application committed the change. |
| Correlation id | Client id that groups field events from one save. |
| Gap | A business change with no stored event. The API returns no row for it. |
