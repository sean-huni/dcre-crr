# dcre-crr

Collections Request Reader: boundary stage that ingests OnHost copybook files into the collections spine (`tx_header` / `tx_entry`).

## What it does

CRR is the first stage of both request DAGs. OnHost drops a fixed-width copybook file into the per-client exchange (`onhost-req/in`), AGT registers the arrival and launches CRR as a short-lived Kubernetes Job with `arrival.id` as the identifying JobParameter (R-16). CRR parses the header, runs the file-fatal structural tier (R-19), then ingests every detail record; it is the single writer of the spine tables (R-04), and every downstream stage transitions via the database, never via files (R-30).

- DC route `onhost-req`: `CRR -> CTV -> { CDE || CIR }`
- ENDO route `onhost-req-endo`: `CRR -> CTV -> AIS -> { CDE || CIR }`

## Architecture and principles

Ephemeral Spring Boot 4.1.0 / Spring Batch 6 / Java 25 batch job, not a server: `ExitCodeMain` wires the Batch outcome into the JVM exit code (R-34) and the job runner fires `crrJob` once per launch. CockroachDB is reached through the PostgreSQL driver.

SOLID as applied here:

- Thin entry adapters: `HeaderTasklet` extracts JobParameters and calls one `HeaderService` method; business logic lives in the service tier, persistence only via `data/repo`.
- One responsibility per unit: `LineRangePartitioner` (byte-offset ranges), `FixedRecordRangeReader` (ISO_8859_1, byte-transparent range read), `SpineWriter` (record parse + content hash), `TxEntryBatchDao` (sole owner of the `tx_entry` write SQL).
- Layer-first packages: `batch/`, `config/`, `data/model/`, `data/repo/`, `service/`.

12FactorApp Alignment - https://12factor.net/ : config strictly from the environment over committed working dev defaults (a clean clone boots with no `.env`), stateless one-shot process, CockroachDB and the exchange directory as attached backing services, dev/prod parity via Testcontainers CockroachDB in the test suite.

### Job structure (`za.co.fnb.dcre.crr.config.CrrJobConfig`)

1. `headerStep` (tasklet): `HeaderTasklet -> HeaderService` parses line 1 against `Layouts.HEADER` (attested content length 109) and runs the structural tier: empty file, header length, layout_version with V1 fail-closed unless `dcre.v1-enabled` (A-2), declared tx_count vs actual lines, R-31 filename-client vs header destination_id. On success it upserts `tx_header` keyed on arrival_id. On failure the step exits `FILE_FATAL`, the reason lands in the execution context as `fileFatalReason`, and the job transitions straight to end COMPLETED: a business verdict, never a process death. The step carries the shared `CrdbRetryExceptionHandler` so commit-time CRDB 40001 serialization aborts re-run the tasklet instead of failing the job.
2. `detailStep` (R-41 partitioned manager/worker pair): `LineRangePartitioner` splits the detail records into contiguous byte-offset ranges (all probed as byte counts so non-UTF-8 bytes never desync the offsets; a ragged length or an LRECL matching no layout is FILE_FATAL). Grid size is `PartitionSizer.partitions(dcre.crr.max-partitions)` (cgroup-aware CPU clamp) and the ranges fan out onto a `VirtualThreadTaskExecutor`. Each worker (`detailWorkerStep`, chunk 100) reads its range with `FixedRecordRangeReader` (restart resumes mid-range via `read.count`, R-05) into `SpineWriter`, which picks the layout by LRECL (`DETAIL_V2` = 169, or `DETAIL_V1` = 161 which fails closed unless V1 is enabled), parses amounts through `MoneyText` at `dcre.amount-scale`, computes a SHA-256 `content_hash` over the essential business fields (for CTV's in-file dup scan), and derives `sequence` from the record's file position (`recordIndex + 1`, never a shared counter, so partitioned ingest is deterministic). The 40001 retry is deliberately NOT registered on the worker step: a swallowed commit-time abort there would silently drop the in-flight chunk (verified empirically 2026-07-14); a worker commit abort stays step-FAILED -> relaunch, which resumes idempotently.

### Idempotent restart semantics

- Identifying JobParameter `arrival.id` (R-16): rerunning the same identity refuses with JobInstanceAlreadyComplete and the spine stays unchanged.
- Writes go through `TxEntryBatchDao`'s guarded `INSERT ... ON CONFLICT (arrival_id, sequence) DO UPDATE`, batched 500 rows per `JdbcTemplate.batchUpdate` (CRDB `UPSERT` arbitrates on the PK only, hence the explicit business-key conflict target).
- `BatchMetaConfig` runs `StaleExecutionSweeper.abandonStale(ds, "CRR_BATCH_", 60)` before the job runner fires (A-39a): a relaunch after a pod kill never throws JobExecutionAlreadyRunning.

### Outcome seam and failure evidence

`CrrJobListener` writes the business-verdict seam file (SYNTHETIC-CONTRACT, R-35) to `<exchange-root>/outcomes/<JOB_NAME>`: `BUSINESS_ACCEPTED` on a clean run, `BUSINESS_FILE_FATAL` when `fileFatalReason` is set. A non-COMPLETED execution writes nothing: the exit code and the K8s Failed condition are the witnesses, and AGT treats absence as never-success (R-33 arbiter clause). The `CRR_BATCH_` metadata is the step-grain diagnostics annex only; AGT never reads it for orchestration decisions.

### Database and batch metadata

Liquibase owns the schema, with per-service history tables (`crr_databasechangelog` / `crr_databasechangeloglock`) on the shared DB:

- `001-spine.xml`: `tx_header` (UNIQUE arrival_id; raw + canonical msg_id per R-15) and `tx_entry` (UNIQUE arrival_id/sequence; amount_raw kept alongside the config-scaled DECIMAL while A-1 is open). Guarded MARK_RAN preconditions so bootstrap converges from any service order.
- `002-batch-metadata.xml`: Liquibase-owned copy of the Spring Batch 6.0.4 postgres DDL (via `sqlFile`, vendored as `batch-metadata-crr.sql`), prefixed `CRR_BATCH_` (`spring.batch.jdbc.table-prefix`, `initialize-schema: never`), EXIT_MESSAGE widened to TEXT so CRDB-driver cause chains are never truncated (A-39b).
- `003-layering.xml`: BaseEntity columns (version, created_at, updated_at).
- `004-content-hash.xml`: nullable `tx_entry.content_hash CHAR(64)` plus the covering index `(arrival_id, content_hash, sequence)` for CTV's per-arrival window dup scan.

Key rules: R-04 single writer, R-05 restart-without-duplication, R-16 launch identity, R-19 file-fatal tier, R-30 boundary file I/O, R-31 filename grammar cross-check, R-33 two-plane failure evidence, R-34 exit-code wiring + prefixed metadata, R-35 synthetic seam contract, R-41 intra-file parallelism + content hash, A-2 V1 fail-closed.

## Prerequisites

- Java 25 (Gradle toolchain, resolved automatically)
- Docker (Testcontainers tests and image build)
- Platform libs `za.co.fnb.dcre:platform-*:0.1.0` published to Maven Local (no remote repository): run `./gradlew publishToMavenLocal` in each platform repo, publish chain `dcre-platform-model` -> `dcre-platform-files` -> `dcre-platform-batch`; `dcre-platform-persistence` is standalone. This repo declares `platform-batch` (`ExitCodeMain`, `OutcomeFileWriter`, `StaleExecutionSweeper`, `CrdbRetryExceptionHandler`, `PartitionSizer`; `Layouts`/`R31Filename`/`MoneyText` arrive transitively via its `api` chain) and `platform-persistence` (`BaseEntity`, `JdbcConfig`).
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
| `DCRE_EXCHANGE_ROOT` | `../../../../../infra/dcre-infra/exchange` | Exchange root for the outcome seam file |
| `DCRE_AMOUNT_SCALE` | `2` | MoneyText scale (SYNTHETIC-CONTRACT while A-1 is open) |
| `DCRE_V1_ENABLED` | `false` | V1 layout gate (A-2: fails closed in production) |
| `DCRE_CRR_MAX_PARTITIONS` | `5` | Upper bound on the detailStep partition grid (R-41); actual grid = clamp(available CPUs, 1, this) |
| `JOB_NAME` | `local-<executionId>` | Set by AGT on the K8s Job; names the outcome seam file |

## Testing

```bash
./gradlew test
```

Docker required: integration tests run on Testcontainers CockroachDB `cockroachdb/cockroach:v26.2.3`.

- `CrrJobTest`: the DC sample parses into 1 header + 30 entries with MoneyText scaling; the same identity refuses a second run without duplicating (R-05/R-16); a V1 file completes as FILE_FATAL with zero details persisted; an unpadded-header file and a non-UTF-8 byte both ingest byte-exactly.
- `CrrPartitionDeterminismTest`: the same fixture under `dcre.crr.max-partitions` 1 vs 5 in two @Nested contexts yields identical `(sequence, e2e, content_hash)` rows (R-41 determinism).
- `CrrJobConfigRetryTest`: headerStep re-runs the tasklet on commit-time CRDB 40001 serialization aborts.
- `LineRangePartitionerTest` / `FixedRecordRangeReaderTest`: byte-offset ranges, missing-final-newline, ragged-length and wrong-separator fail-closed, mid-range restart.
- Cucumber BDD suite (`CucumberSuiteTest`, `features/crr_boundary_reader.feature`, tag `@crr`): business-language scenarios over the real job + CockroachDB.

Fixtures: `src/test/resources/dcre_copybook_v{1,2}_*.txt`.

## Local cluster deployment

```bash
./gradlew bootJar
docker build -t dcre-crr:2.1.1 .
kind load docker-image --name dcre-dev dcre-crr:2.1.1
```

Image base: `eclipse-temurin:25-jre-alpine`. Image tags follow the fleet release tags (digits-only SemVer, current `2.1.1`); the Gradle project version inside the jar name stays `2.0`.

AGT launches CRR as an ephemeral K8s Job per registered arrival: the image comes from AGT's `AGT_CRR_IMAGE` env (dcre-infra `scripts/switch-version.sh <version>` points it at `dcre-crr:<version>` fleet-wide), the JobParameters arrive as program args, and `JOB_NAME` is set in the Job env. Cluster bootstrap and clean-slate resets: dcre-infra `scripts/kind-up.sh` and `scripts/env-reset.sh`.

## Related repositories

- Orchestrator: [dcre-agt](https://github.com/sean-huni/dcre-agt)
- Request DAG stages: [dcre-crr](https://github.com/sean-huni/dcre-crr) (this repo), [dcre-ctv](https://github.com/sean-huni/dcre-ctv), [dcre-cde](https://github.com/sean-huni/dcre-cde), [dcre-cir](https://github.com/sean-huni/dcre-cir), [dcre-ais](https://github.com/sean-huni/dcre-ais)
- Clock/response stages: [dcre-crw](https://github.com/sean-huni/dcre-crw), [dcre-ixr](https://github.com/sean-huni/dcre-ixr), [dcre-sxr](https://github.com/sean-huni/dcre-sxr), [dcre-pxr](https://github.com/sean-huni/dcre-pxr), [dcre-prg](https://github.com/sean-huni/dcre-prg), [dcre-hcs](https://github.com/sean-huni/dcre-hcs)
- Platform libs: [dcre-platform-model](https://github.com/sean-huni/dcre-platform-model), [dcre-platform-files](https://github.com/sean-huni/dcre-platform-files), [dcre-platform-batch](https://github.com/sean-huni/dcre-platform-batch), [dcre-platform-persistence](https://github.com/sean-huni/dcre-platform-persistence)
- Support: [dcre-infra](https://github.com/sean-huni/dcre-infra), [dcre-fixture-toolkit](https://github.com/sean-huni/dcre-fixture-toolkit), [dcre-design-register](https://github.com/sean-huni/dcre-design-register), [dcre-rpt](https://github.com/sean-huni/dcre-rpt)
