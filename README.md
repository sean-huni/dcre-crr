# dcre-crr

> Part of the DCRE fleet. For the fleet map, the rulings and the diagrams that specify every stage, start at the [DCRE design register](https://github.com/sean-huni/dcre-design-register); the complete list of live repositories is its [Repositories](https://github.com/sean-huni/dcre-design-register/blob/dev/README.md#repositories) table.

Collections Request Reader: boundary stage that ingests OnHost copybook files into the collections spine (`tx_header` / `tx_entry`).

## What it does

| | |
|---|---|
| Stage | `CRR` |
| Family / leg | Collections (DC), REQ |
| Trigger | arrival-launched: a copybook file on the `onhost-req` route; CRR is the DAG entry |
| Upstream | none (OnHost drops the file into the per-client `onhost-req/in`) |
| Downstream | `CTV` (DAG `CRR -> CTV -> {CDE, CIR}`) |
| Diagram sheet | `dcre-collections-req` |

DAG position per AGT `RouteDags.DC` on origin/dev (checked 2026-09-28). CRR is the entry of the collections request DAG only: the payments family has its own reader, PRR, on the `onhost-req-endo` route. AGT registers the arrival and launches CRR as a short-lived Kubernetes Job with `arrival.id` as the identifying JobParameter (R-16). CRR parses the header, runs the file-fatal structural tier (R-19), then ingests every detail record; it is the single writer of the spine tables (R-04), and every downstream stage transitions via the database, never via files (R-30).

CRR carries no flow discriminator: with one database per family every row in `dcre_col` is a collection. AGT no longer passes a `flow` launch arg (AGT `JobLauncher.serviceArgs`, checked 2026-09-28); `crrJob` registers no `JobParametersValidator`, so an unread parameter would be ignored rather than fail the launch.

## Architecture and principles

Ephemeral Spring Boot 4.1.0 / Spring Batch 6 / Java 25 batch job, not a server: `ExitCodeMain` wires the Batch outcome into the JVM exit code (R-34) and the job runner fires `crrJob` once per launch. CockroachDB is reached through the PostgreSQL driver.

SOLID as applied here:

- Thin entry adapters: `HeaderTasklet` extracts JobParameters and calls one `HeaderService` method; business logic lives in the service tier, persistence only via `data/repo`.
- One responsibility per unit: `LineRangePartitioner` (byte-offset ranges), `FixedRecordRangeReader` (ISO_8859_1, byte-transparent range read), `CollectionRecords` (LRECL to layout resolution over platform-copybook `Layouts`), `SpineWriter` (record parse + content hash), `TxEntryBatchDao` (sole owner of the `tx_entry` write SQL).
- Layer-first packages: `batch/`, `config/`, `data/model/`, `data/repo/`, `service/`.

12FactorApp Alignment - https://12factor.net/ : config strictly from the environment over committed working dev defaults (a clean clone boots with no `.env`), stateless one-shot process, CockroachDB and the exchange directory as attached backing services, dev/prod parity via Testcontainers CockroachDB in the test suite.

### Job structure (`za.co.fnb.dcre.crr.config.CrrJobConfig`)

1. `headerStep` (tasklet): `HeaderTasklet -> HeaderService` parses line 1 against `Layouts.HEADER` (attested content length 109) and runs the structural tier: empty file, header length, layout_version with V1 fail-closed unless `dcre.v1-enabled` (A-2), declared tx_count vs actual lines, R-31 filename-client vs header destination_id. On success it upserts `tx_header` keyed on arrival_id. On failure the step exits `FILE_FATAL`, the reason lands in the execution context as `fileFatalReason`, and the job transitions straight to end COMPLETED: a business verdict, never a process death. The step carries the shared `CrdbRetryExceptionHandler` so commit-time CRDB 40001 serialization aborts re-run the tasklet instead of failing the job.
2. `detailStep` (R-41 partitioned manager/worker pair): `LineRangePartitioner` splits the detail records into contiguous byte-offset ranges (all probed as byte counts so non-UTF-8 bytes never desync the offsets; a ragged length or an LRECL matching no layout is FILE_FATAL). Grid size is `PartitionSizer.partitions(dcre.crr.max-partitions)` (cgroup-aware CPU clamp) and the ranges fan out onto a `VirtualThreadTaskExecutor`. Each worker (`detailWorkerStep`, chunk 100) reads its range with `FixedRecordRangeReader` (restart resumes mid-range via `read.count`, R-05) into `SpineWriter`, which picks the layout by LRECL through `CollectionRecords` (`DETAIL_V3` = `DETAIL_V2` + a 35-char `mandate_ref`, `DETAIL_V2` = 169, or `DETAIL_V1` = 161 which fails closed unless V1 is enabled; any other length is FILE_FATAL), parses amounts through `MoneyText` at `dcre.amount-scale`, computes a SHA-256 `content_hash` over the essential business fields (for CTV's in-file dup scan), and derives `sequence` from the record's file position (`recordIndex + 1`, never a shared counter, so partitioned ingest is deterministic). The 40001 retry is deliberately NOT registered on the worker step: a swallowed commit-time abort there would silently drop the in-flight chunk (verified empirically 2026-07-14); a worker commit abort stays step-FAILED -> relaunch, which resumes idempotently.

### Idempotent restart semantics

- Identifying JobParameter `arrival.id` (R-16): rerunning the same identity refuses with JobInstanceAlreadyComplete and the spine stays unchanged.
- Writes go through `TxEntryBatchDao`'s guarded `INSERT ... ON CONFLICT (arrival_id, sequence) DO UPDATE`, batched 500 rows per `JdbcTemplate.batchUpdate` (CRDB `UPSERT` arbitrates on the PK only, hence the explicit business-key conflict target).
- `BatchMetaConfig` runs `StaleExecutionSweeper.abandonStale(ds, "CRR_BATCH_", 60)` before the job runner fires (A-39a): a relaunch after a pod kill never throws JobExecutionAlreadyRunning.

### Outcome seam and failure evidence

platform-batch's `OutcomeSeamListener` (bean in `CrrJobConfig`) writes the business-verdict seam file (SYNTHETIC-CONTRACT, R-35) to `<exchange-root>/outcomes/<JOB_NAME>`: `BUSINESS_ACCEPTED` on a clean run, `BUSINESS_FILE_FATAL` when `fileFatalReason` is set. A non-COMPLETED execution writes nothing: the exit code and the K8s Failed condition are the witnesses, and AGT treats absence as never-success (R-33 arbiter clause). The `CRR_BATCH_` metadata is the step-grain diagnostics annex only. The job also registers platform-batch's `HeartbeatWriter`, which stamps liveness on `agt_ops.launch_intent` over its own datasource.

### Database and batch metadata

| Datasource | Database (dev default) | Env vars | Access |
|---|---|---|---|
| primary | `dcre_col` | `DCRE_DB_URL`, `DCRE_DB_USER`, `DCRE_DB_PASSWORD` | read/write |
| heartbeat (platform-batch) | `agt_ops` | `DCRE_AGTOPS_DB_URL`, `DCRE_AGTOPS_DB_USER`, `DCRE_AGTOPS_DB_PASSWORD` | `HeartbeatWriter` liveness stamp |

Writes: `tx_header`, `tx_entry` (single writer) and the `CRR_BATCH_*` tables. Reads: nothing beyond its own tables and the input file.

Liquibase owns the schema, with per-service history tables (`crr_databasechangelog` / `crr_databasechangeloglock`):

The changelog is a **version 1 baseline**: every DCRE database is dropped and recreated at the v1 cutover, so the pre-v1 `2026/07` ladder is gone rather than superseded and the usual re-run guards (ANY-checksum overrides, `IF NOT EXISTS`, `onFail="MARK_RAN"`) are deliberately absent. The root master includes the per-month sub-master `2026/08/db.changelog-2026-08.xml`, never individual changesets.

- `2026/08/001-batch-metadata.xml`: Liquibase-owned Spring Batch 6.0.4 job-repository DDL in pure typed tags, prefixed `CRR_BATCH_` (`dcre.batch.table-prefix`), EXIT_MESSAGE widened to TEXT so CRDB-driver cause chains are never truncated (A-39b).
- `2026/08/002-spine.xml`: `tx_header` (UNIQUE arrival_id; raw + canonical msg_id per R-15) and `tx_entry` (UNIQUE arrival_id/sequence; amount_raw kept alongside the config-scaled DECIMAL while A-1 is open), including the BaseEntity columns (version, created_at, updated_at), the nullable `content_hash CHAR(64)` with its covering index `(arrival_id, content_hash, sequence)` for CTV's per-arrival window dup scan, and the nullable `mandate_ref` that only DETAIL_V3 carries (M10).

Key rules: R-04 single writer, R-05 restart-without-duplication, R-16 launch identity, R-19 file-fatal tier, R-30 boundary file I/O, R-31 filename grammar cross-check, R-33 two-plane failure evidence, R-34 exit-code wiring + prefixed metadata, R-35 synthetic seam contract, R-41 intra-file parallelism + content hash, A-2 V1 fail-closed.

## Prerequisites

- Java 25 (`.sdkmanrc`: `java=25-tem`; `build.gradle` sets source/target compatibility 25; no Gradle toolchain resolution)
- Gradle 9.5.1 via the wrapper
- Docker (Testcontainers tests and image build)
- Platform libs published to Maven Local (no remote repository): run `./gradlew publishToMavenLocal` in each platform repo, publish chain `dcre-platform-model` -> `dcre-platform-files` -> `dcre-platform-batch`; `dcre-platform-persistence` and `dcre-platform-copybook` alongside. This repo declares `platform-batch:0.1.0` (`ExitCodeMain`, `BatchJdbcConfig`, `HeartbeatDatasourceConfig`, `HeartbeatWriter`, `OutcomeSeamListener`, `StaleExecutionSweeper`, `CrdbRetryExceptionHandler`, `PartitionSizer`; `R31Filename`, `MoneyText` and `OpaqueRef` arrive transitively via its `api` chain), `platform-copybook:0.2.0` (`Layouts`, `CopybookReader`, `FixedWidthLayout`, `LayoutResolver`) and `platform-persistence:0.1.0` (`BaseEntity`, `JdbcConfig`).
- A reachable CockroachDB for a real local run (committed default: `localhost:26257`, database `dcre_col`); the dcre-infra kind cluster provides one.

## Quickstart

Clean clone, no `.env` needed (working dev defaults are committed in `application.yml`):

```bash
./gradlew test          # full suite, Docker required
./gradlew bootJar       # build/libs/crr-2.0.jar

# local one-shot run against a reachable CockroachDB
java -jar build/libs/crr-2.0.jar \
  'arrival.id=<uuid>,java.lang.String,true' \
  'input.file=/path/to/file.txt,java.lang.String,false' \
  'original.name=FNBRF01_DCRERF2026071112000002.txt,java.lang.String,false'
```

## Configuration

Env over committed dev defaults (precedence: yml default < environment).

| Env | Default | Purpose |
|---|---|---|
| `DCRE_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_col?sslmode=disable` | Shared collections DB (CockroachDB) |
| `DCRE_DB_USER` | `root` | DB user |
| `DCRE_DB_PASSWORD` | (empty) | DB password |
| `DCRE_AGTOPS_DB_URL` | `jdbc:postgresql://localhost:26257/agt_ops?sslmode=disable` | heartbeat datasource |
| `DCRE_AGTOPS_DB_USER` / `DCRE_AGTOPS_DB_PASSWORD` | `root` / (empty) | heartbeat credentials |
| `DCRE_EXCHANGE_ROOT` | `../../../../../../infra/dcre-infra/exchange` | Exchange root for the outcome seam file (AGT sets `/exchange`) |
| `DCRE_AMOUNT_SCALE` | `2` | MoneyText scale (SYNTHETIC-CONTRACT while A-1 is open) |
| `DCRE_V1_ENABLED` | `false` | V1 layout gate (A-2: fails closed in production) |
| `DCRE_CRR_MAX_PARTITIONS` | `5` | Upper bound on the detailStep partition grid (R-41); actual grid = clamp(available CPUs, 1, this) |
| `JOB_NAME` | `local-crr-<executionId>` | Set by AGT on the K8s Job; names the outcome seam file |

This is the documented set, not a closed total: Spring relaxed binding lets any Spring or `dcre.*` property be overridden by its derived environment variable name.

## Testing

```bash
./gradlew test
```

Docker required: integration tests run on Testcontainers CockroachDB `cockroachdb/cockroach:v26.2.3`.

- `CrrJobTest`: the DC sample parses into 1 header + 30 entries with MoneyText scaling; the same identity refuses a second run without duplicating (R-05/R-16); a V1 file completes as FILE_FATAL with zero details persisted; an unpadded-header file and a non-UTF-8 byte both ingest byte-exactly.
- `CrrPartitionDeterminismTest`: the same fixture under `dcre.crr.max-partitions` 1 vs 5 in two @Nested contexts yields identical `(sequence, e2e, content_hash)` rows (R-41 determinism).
- `CrrJobConfigRetryTest`: headerStep re-runs the tasklet on commit-time CRDB 40001 serialization aborts.
- `SpineWriterTest` (unit): content hash covers the essential fields only and changes with the amount; `mandate_ref` mapped from DETAIL_V3, NULL for DETAIL_V2.
- `LineRangePartitionerTest` / `FixedRecordRangeReaderTest`: byte-offset ranges, missing-final-newline, ragged-length and wrong-separator fail-closed, mid-range restart.
- Cucumber BDD suite (`CucumberSuiteTest`, `features/crr_boundary_reader.feature`, tag `@crr`): business-language scenarios over the real job + CockroachDB.

Fixtures: `src/test/resources/dcre_copybook_v{1,2}_*.txt`.

## Local cluster deployment

```bash
VERSION=<fleet release tag>
./gradlew bootJar
docker build -t dcre-crr:$VERSION .
kind load docker-image --name dcre-dev dcre-crr:$VERSION
```

Image base: `eclipse-temurin:25-jre-alpine`. Image tags follow the fleet release tags (digits-only SemVer); the Gradle project version inside the jar name stays `2.0`.

AGT reads the image from `AGT_CRR_IMAGE` (empty by default, which leaves the stage launch-disabled); dcre-infra `scripts/switch-version.sh <version>` sets it to `dcre-crr:<version>` (checked 2026-09-28). Per arrival AGT creates a Job in the collections flow namespace (AGT `AGT_NAMESPACE_COL`, default `dcre-col`) with program args `arrival.id=<uuid>`, `input.file=<claimed path>`, `original.name=<physical filename>` and env `JOB_NAME`, `DCRE_DB_URL` (AGT `service-db-url`, `dcre_col`), `DCRE_EXCHANGE_ROOT=/exchange`, `DCRE_AGTOPS_DB_URL`, `DCRE_AGTOPS_DB_USER` (AGT `JobLauncher` on origin/dev, checked 2026-09-28). Cluster bootstrap and clean-slate resets: dcre-infra `scripts/kind-up.sh` and `scripts/env-reset.sh`.

## Related repositories

The complete, current list of live DCRE repositories (stage services, orchestrator, platform libraries, infra and tooling) lives in one place: the [DCRE design register README](https://github.com/sean-huni/dcre-design-register/blob/dev/README.md#repositories). Deprecated and archived repositories are deliberately absent from it. This README does not copy that list, so it cannot drift.

- Design register: https://github.com/sean-huni/dcre-design-register (start at `docs/specs/DESIGN-REGISTER.md`; the diagrams in `docs/diagrams/` are the specification)
