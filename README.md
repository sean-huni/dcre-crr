# dcre-crr

Collection Request Reader: OnHost copybook FlatFile to the collections spine (tx_header/tx_entry). Boundary reader per R-30. Header tasklet runs the file-fatal structural tier (length, version incl. V1 fail-closed per A-2, declared count, R-31 filename-vs-header) and routes FILE_FATAL as a business verdict (job COMPLETED, seam file); the chunked detail step upserts tx_entry keyed (arrival_id, sequence) so restarts never duplicate (R-05). Batch metadata in prefixed CRR_BATCH_ tables via a Liquibase-owned copy of the Batch 6 DDL with EXIT_MESSAGE widened (A-39b); stale STARTED executions self-abandoned at startup (A-39a). Identifying JobParameter: arrival.id (R-16).

Spring Boot 4.1.0 / Spring Batch 6 / Java 25, CockroachDB via the PostgreSQL driver. Ephemeral batch job, not a server: `ExitCodeMain` wires the Batch outcome into the JVM exit code (R-34).

## Pipeline position

First stage of both request DAGs (SPEC-DAG-PIPELINE): OnHost drops the copybook FlatFile into the exchange, AGT registers the arrival and launches CRR as an ephemeral K8s Job.

- DC route `onhost-req`: `CRR -> CTV -> { CDE || CIR }`
- ENDO route `onhost-req-endo`: `CRR -> CTV -> AIS -> { CDE || CIR }`

CRR is the single writer of the spine tables (R-04); every downstream stage transitions via the database, never via files (R-30).

## Job structure

`crrJob` (za.co.fnb.dcre.crr.config.CrrJobConfig), two steps:

1. `headerStep` (tasklet): `HeaderTasklet -> HeaderService` parses line 1 against `Layouts.HEADER` (attested content length 109) and runs the structural tier: empty file, header length, layout_version with V1 fail-closed unless `dcre.v1-enabled` (A-2), declared tx_count vs actual lines, R-31 filename-client vs header destination_id. On success it upserts `tx_header` keyed on arrival_id. On failure the step exits `FILE_FATAL`, the reason lands in the execution context as `fileFatalReason`, and the job transitions straight to end COMPLETED: a business verdict, never a process death.
2. `detailStep` (chunk 100): `FlatFileItemReader` (skips the header line) feeds `SpineWriter`, which picks the layout by LRECL (`DETAIL_V2`, or `DETAIL_V1` = 161 which fails closed unless V1 is enabled; any other length is FILE_FATAL), parses amounts through the single `MoneyText` converter at `dcre.amount-scale`, and upserts `tx_entry` via a guarded native `INSERT ... ON CONFLICT (arrival_id, sequence) DO UPDATE` (CRDB `UPSERT` keys only on the PK, hence the explicit conflict target).

`CrrJobListener` writes the business-verdict seam file (SYNTHETIC-CONTRACT, R-35) to `<exchange-root>/outcomes/<JOB_NAME>`: `BUSINESS_ACCEPTED` on a clean run, `BUSINESS_FILE_FATAL` when `fileFatalReason` is set. A non-COMPLETED execution writes nothing: the exit code and the K8s Failed condition are the witnesses, and AGT treats absence as never-success (R-33 arbiter clause).

JobParameters: `arrival.id` (identifying, UUID from AGT's arrival registry, R-16), `input.file` and `original.name` (non-identifying). Rerunning the same identity refuses with JobInstanceAlreadyComplete and the spine stays unchanged.

## Key rules

R-04 single writer, R-05 restart-without-duplication (upsert identity arrival_id/sequence), R-16 launch identity, R-19 file-fatal tier, R-30 boundary file I/O, R-31 filename grammar cross-check, R-33 two-plane failure evidence, R-34 exit-code wiring + prefixed metadata, R-35 synthetic seam contract, A-2 V1 fail-closed.

## Local module dependencies

| Module | Version | Scope | Used for |
|---|---|---|---|
| `dcre-platform-persistence` | 0.1.0 | `implementation` | `BaseEntity` (version/created_at/updated_at on `TxHeaderEntity`/`TxEntryEntity`), `JdbcConfig` (Spring Data JDBC base config, imported by `CrrApplication`) |
| `dcre-platform-batch` | 0.1.0 | `implementation` | `ExitCodeMain` (R-34 exit-code wiring), `OutcomeFileWriter` (outcome seam), `StaleExecutionSweeper` (A-39a self-abandonment) |

`Layouts`/`FixedWidthLayout`/`R31Filename` (`dcre-platform-files`) and `MoneyText`/`OpaqueRef` (`dcre-platform-model`) are not declared directly: they arrive transitively via `dcre-platform-batch`'s `api` chain (batch brings files brings model). All artifacts resolve from Maven Local only (no remote repository): run `./gradlew publishToMavenLocal` in each dependency repo first, publish chain `dcre-platform-model` -> `dcre-platform-files` -> `dcre-platform-batch`; `dcre-platform-persistence` is standalone. Details in each module repo's README under "Publishing".

## Configuration (env over committed dev defaults, 12FactorApp Alignment: https://12factor.net/)

| Env | Default | Meaning |
|---|---|---|
| `DCRE_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_collections?sslmode=disable` | Shared collections DB (CockroachDB) |
| `DCRE_DB_USER` / `DCRE_DB_PASSWORD` | `root` / empty | DB credentials |
| `DCRE_EXCHANGE_ROOT` | `../../infra/dcre-infra/exchange` | Exchange root for the outcome seam |
| `DCRE_AMOUNT_SCALE` | `2` | MoneyText scale (SYNTHETIC-CONTRACT while A-1 is open) |
| `DCRE_V1_ENABLED` | `false` | V1 layout gate (A-2: fails closed in production) |
| `JOB_NAME` | `local-<executionId>` | Set by AGT on the K8s Job; names the outcome seam file |

Clean clone runs with no `.env` at all; the working dev defaults are committed in `application.yml`.

## Database & batch metadata

Liquibase owns the schema, with per-service history tables (`crr_databasechangelog` / `crr_databasechangeloglock`) on the shared DB:

- `001-spine.xml`: `tx_header` (UNIQUE arrival_id; raw + canonical msg_id per R-15) and `tx_entry` (UNIQUE arrival_id/sequence; raw + canonical e2e; amount_raw kept alongside the config-scaled DECIMAL while A-1 is open). Guarded MARK_RAN preconditions so bootstrap converges from any service order (dcre-prg mints the same tables IF NOT EXISTS).
- `002-batch-metadata.xml`: Liquibase-owned copy of the Spring Batch 6.0.4 postgres DDL, prefixed `CRR_BATCH_` (`spring.batch.jdbc.table-prefix`, `initialize-schema: never`), EXIT_MESSAGE widened to TEXT so CRDB-driver cause chains are never truncated (A-39b).
- `003-layering.xml`: BaseEntity columns (version, created_at, updated_at).

`BatchMetaConfig` runs `StaleExecutionSweeper.abandonStale(ds, "CRR_BATCH_", 60)` before the job runner fires (A-39a): a relaunch after a pod kill never throws JobExecutionAlreadyRunning. AGT never touches service metadata.

## Build & test

`./gradlew test` (needs Docker): `CrrJobTest` on Testcontainers CockroachDB v26.2.3 proves the DC sample parses into 1 header + 30 entries with MoneyText scaling, that the same identity refuses a second run without duplicating (R-05/R-16), and that a V1 file completes as FILE_FATAL with zero details persisted. Fixtures: `src/test/resources/dcre_copybook_v{1,2}_*.txt`. Platform libs resolve from mavenLocal (see Local module dependencies).

## Run

Cluster: AGT launches CRR as an ephemeral K8s Job per registered arrival (image: `./gradlew bootJar && docker build -t dcre-crr:0.1.0 .`, eclipse-temurin 25 jre-alpine), passing the JobParameters as program args and `JOB_NAME` in the env. Local one-shot:

```bash
./gradlew bootJar
java -jar build/libs/dcre-crr-0.1.0.jar \
  'arrival.id=<uuid>,java.lang.String,true' \
  'input.file=/path/to/file.txt,java.lang.String,false' \
  'original.name=FNBRF01_DCRERF2026071112000002.txt,java.lang.String,false'
```

## Observability

No metrics endpoints yet. The observable surface is: the JVM exit code (R-34), the outcome seam file (R-35), and the `CRR_BATCH_` metadata as the step-grain diagnostics annex (R-33: advisory only, AGT never reads it for orchestration decisions).
