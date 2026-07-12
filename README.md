# dcre-prg

Payment Report Generator (SCRUM-28, M4). Projects per-transaction external status (ext_tx_status view over spine + validation + ISR/SBSR/PBSR response legs, R-17 stage ranking) and emits delta PSR report files per client on clock windows. Watermark keyed (client, e2e): a window with no status changes emits NO file; resend=true re-emits all current rows. R-29 order: file first (StagedWrite), watermark advance second. 3-tier: PsrTasklet -> PsrReportService -> data/repo. PSR flat file format is SYNTHETIC-CONTRACT (R-35). Spring Boot 4.1.0 / Spring Batch / Java 25.

## Pipeline position

Terminal stage of the shared response leg: `IXR | SXR | PXR -> ext_tx_status -> PRG`, serving DC and ENDO alike. Not file-triggered: AGT's clock instantiates `prgJob` per (client, window) from the per-client report schedule (R-28), plus on-demand runs with the same launch contract; PRG never waits on the response readers, it reports whatever `ext_tx_status` holds behind the watermark. Output: PSR FlatFile to the OnHost response directory (R-30 boundary writer). PRG is canonical naming; never write CRG (R-13).

## Job structure

One job `prgJob`, one tasklet step `psrStep`; 3-tier `PsrTasklet -> PsrReportService -> PrgWatermarkRepo` (+ row mappers).

- Job parameters: `client` and `window` (both identifying: job identity is (client, window), R-16); `resend` (non-identifying, `"true"` to override the watermark).
- Scheduled run: delta selection, rows whose `ext_tx_status.status` moved past `prg_watermark.last_status` (or have no watermark row yet), `status IS NOT NULL` only. Zero delta rows = no file (the ExecutionContext records `psr.file=NONE`).
- Resend run: ALL current known-status rows for the client, watermark ignored; re-projects CURRENT state, not the original report (A-8 ruling).
- R-38 exclusion visibility: mid-DAG rows with `status IS NULL` are never reportable; each is WARN-logged in the uniform shape `excluded stage=PRG arrival=<uuid> seq=<n> e2e=<e2e> reason=STATUS_UNKNOWN` and leaves the watermark untouched.
- File: `<exchange-root>/onhost-resp/<client>_PSR_<window>.txt`; layout SYNTHETIC-CONTRACT (R-35): header `PSR|client|window`, one `TX|e2e|status` per row ordered by e2e, trailer `END|count`.
- R-29 order: `StagedWrite` (tmp + ATOMIC_MOVE) first, then per-row watermark upsert `ON CONFLICT (client, e2e) DO UPDATE`. A crash between the two replays as a StagedWrite no-op plus watermark advance: neither skips nor duplicates (R-05). Never CRDB `UPSERT INTO`: it resolves on PK only, business identity is (client, e2e).
- Outcome seam: on COMPLETED, `BUSINESS_ACCEPTED` to `<exchange-root>/outcomes/<JOB_NAME>` (`OutcomeFileWriter`, R-33).

## Database and batch metadata

Liquibase (per-service history tables `prg_databasechangelog` / `prg_databasechangeloglock` on the shared `dcre_collections` DB):

1. `001-prg.xml`, changeSet 001: BOOTSTRAP-ORDER GUARD. PRG is clock-launched and may run on a fresh DB before CRR/CTV and the response readers ever executed, so every view source (`tx_header`, `tx_entry`, `validation_log`, `isr_resp`, `sbsr_resp`, `pbsr_resp`) is created `IF NOT EXISTS` with the owners' exact column sets.
2. ChangeSet 002: `ext_tx_status` view. Deepest response leg wins (R-17 stage rank PBSR 4 > SBSR 3 > ISR 2 > CTV 1); a PASS validation with no response yet projects as `CTV_PASS`, a FAIL projects its outcome verbatim; client = `tx_header.client_token`.
3. ChangeSet 003: `prg_watermark` (PRG single writer, R-04), `UNIQUE (client, e2e)`, `last_status NOT NULL`.
4. `002-batch-metadata.xml` -> `batch-metadata-prg.sql`: Spring Batch metadata under prefix `PRG_BATCH_` (`initialize-schema: never`). `StaleExecutionSweeper.abandonStale(ds, "PRG_BATCH_", 60)` runs as an `@Order(-10)` `ApplicationRunner` before launch (A-39a).

## Local module dependencies

| Module | Version | Scope | Used for |
|---|---|---|---|
| `dcre-platform-persistence` | 0.1.0 | `implementation` | `BaseEntity` (version/created_at/updated_at on `PrgWatermarkEntity`), `JdbcConfig` (Spring Data JDBC base config, imported by `PrgApplication`) |
| `dcre-platform-files` | 0.1.0 | `implementation` | `StagedWrite` (R-29 file-first atomic PSR write: tmp + ATOMIC_MOVE, restart no-op) |
| `dcre-platform-batch` | 0.1.0 | `implementation` | `ExitCodeMain` (R-34 exit-code wiring), `OutcomeFileWriter` (outcome seam), `StaleExecutionSweeper` (A-39a self-abandonment) |

All three resolve from Maven Local only (no remote repository): run `./gradlew publishToMavenLocal` in each dependency repo first, publish chain `dcre-platform-model` -> `dcre-platform-files` -> `dcre-platform-batch` (each `api`-exposes the previous, so batch brings files brings model); `dcre-platform-persistence` is standalone. Details in each module repo's README under "Publishing".

## Configuration

12FactorApp: committed working dev defaults, env overrides, clean clone runs with no `.env`.

| Env | Default | Use |
|---|---|---|
| `DCRE_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_collections?sslmode=disable` | CockroachDB via pgwire |
| `DCRE_DB_USER` / `DCRE_DB_PASSWORD` | `root` / empty | DB credentials |
| `DCRE_EXCHANGE_ROOT` | `../../infra/dcre-infra/exchange` | PSR output dir + outcome seam |
| `DCRE_AMOUNT_SCALE` | `2` | Fleet-wide flag; not read by PRG sources |
| `DCRE_V1_ENABLED` / `DCRE_FLOW_DC` | `false` / `true` | Fleet-wide flags; not read by PRG sources |
| `JOB_NAME` | `local-<executionId>` | K8s-injected identity for the outcome seam |

## Build and test

`./gradlew build` (Gradle 9.5.1 wrapper, Java 25 toolchain). Platform libs resolve from mavenLocal (see Local module dependencies). `./gradlew test`: Testcontainers CockroachDB v26.2.3, one end-to-end window sequence: (a) first delta carries the deepest-leg status per row (R-17) and exactly one R-38 exclusion WARN for the mid-DAG row, (b) unchanged window emits no file, (c) a single status flip emits exactly that row, (d) resend re-emits all current rows.

## Run

One-shot batch process; the JVM exit code carries the Batch outcome (`ExitCodeMain`, R-34).

```bash
./gradlew build
java -jar build/libs/dcre-prg-0.1.0.jar client=<client> window=<windowKey>          # scheduled delta
java -jar build/libs/dcre-prg-0.1.0.jar client=<client> window=<windowKey> resend=true  # resend override
```

Container: `docker build -t dcre-prg:dev .` (eclipse-temurin:25-jre-alpine). In the cluster AGT launches the image per (client, window) as an ephemeral K8s Job with `JOB_NAME` set; the JobRepository dedupes on the identifying pair (restart-not-duplicate, R-16). Requires a reachable CockroachDB and the exchange directory (`dcre-infra` compose stack locally).

## Observability

No metrics wiring yet (no actuator/Micrometer dependency). The R-38 exclusion WARNs are the contractually shaped log surface; outcome seam files plus `PRG_BATCH_` metadata carry run state.
