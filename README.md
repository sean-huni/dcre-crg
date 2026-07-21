# dcre-prg

PSR Generator: clock-windowed status reporter, the terminal stage of the DCRE response leg.

## What it does

PRG projects per-transaction external status (the `ext_tx_status` view over spine + validation + ISR/SBSR/PBSR response legs, deepest leg wins per R-17) and emits delta Payment Status Report (PSR) files per client on clock windows. Each run diffs `ext_tx_status` against the `prg_watermark` table for one client, streams the delta as a PSR flat file into the client's `onhost-resp/out` exchange directory, then advances the watermark in bounded slices. It is not file-triggered: AGT's clock instantiates `prgJob` per (client, window) as a short-lived Kubernetes Job, and PRG reports whatever `ext_tx_status` holds behind the watermark, serving the DC and ENDO flows alike.

## Architecture and principles

- **SOLID, 3-tier**: one responsibility per class along `PsrTasklet` (thin Spring Batch entry adapter) -> `PsrReportService` (business tier) -> `PrgWatermarkRepo` (Spring Data JDBC). Supporting single-purpose units: `StreamedPsrWrite` (staged streaming boundary write), `CrdbRetry` (bounded SQLSTATE 40001 retry), `SeamListener` (outcome seam). Layer-first packages: `config/`, `service/`, `data/model/`, `data/repo/`.
- **12FactorApp Alignment - https://12factor.net/**: config strictly from the environment with committed working dev defaults (a clean clone runs with no `.env`), stateless one-shot process (the JVM exit code carries the Batch verdict via `ExitCodeMain`, R-34), CockroachDB and the exchange directory as attached resources.
- **Idempotent restart semantics**: job identity is the identifying parameter pair (client, window) (R-16); `resend` is non-identifying. R-29 order: the WHOLE file becomes visible first (streamed tmp + `ATOMIC_MOVE`), then watermarks advance in per-slice `REQUIRES_NEW` transactions. An existing target file is a restart no-op (R-24); a crash between file and watermark replays as skip-existing-file + watermark advance, neither skipping nor duplicating. `StaleExecutionSweeper.abandonStale(ds, "PRG_BATCH_", 60)` runs as an `@Order(-10)` `ApplicationRunner` so a killed pod never strands a STARTED execution (A-39a).
- **CRDB-correct upserts**: watermark writes are `INSERT ... ON CONFLICT (client, e2e) DO UPDATE`, never `UPSERT INTO` (CRDB arbitrates UPSERT on the primary key only; the business identity is (client, e2e)). Serialization aborts (40001) retry up to 5 attempts with jittered backoff in a fresh transaction per attempt.
- **Bounded scale (SCRUM-42)**: whole-book reads plus a full in-heap render blew CRDB's sql memory budget on the 30M-tx book. Every read is now a keyset slice (`ORDER BY e2e LIMIT :limit`, default 50000) and the PSR streams to disk slice by slice; nothing holds more than one slice in heap.

### Fintegrate status classification

`prg_status_class` is the runtime authority. All fourteen recognised codes are explicit:

- Terminal success: `ACSC`, `ACCC`.
- Terminal non-success: `RJCT`, `CANC`.
- Accepted non-terminal: `ACSP`, `ACTC`, `ACCP`, `ACFC`.
- Pending/interim: `RCVD`, `PDNG`, `PART`, `PATC`.
- Accepted warehoused (SLA-suppressed): `ACWP` future-dated, `ACWC` auto-bumped per the RMB
  DebiCheck profile.

Accepted warehoused codes (SCRUM-68, migration `2026/07/005`) are reportable non-terminal
statuses: the transaction has its response, so it enters PSRs normally, but it is suppressed from
the 20h/24h pending-SLA path via `prg_status_class.sla_suppressed` (`prg_sla_pending` excludes
suppressed codes; otherwise every future-dated collection would false-amber).

Unknown Fintegrate codes are preserved unchanged but fail closed as non-terminal, non-reportable
protocol exceptions. They appear in `prg_status_exception`, remain visible to the 20h/24h
pending-SLA path, and never enter scheduled, resend, immediate or manual-regeneration PSRs.
DCRE never translates them into a supported code and never instructs OnHost to resubmit until
Fintegrate confirms that no processing or settlement occurred. Exact historical report replay is
unchanged because it replays the immutable delivery ledger rather than current status projection.

### Job contract

One job `prgJob`, one tasklet step `psrStep`.

- Scheduled run: delta selection, reportable rows whose `ext_tx_status.status` moved past `prg_watermark.last_status` (or have no watermark row yet). Unknown Fintegrate response codes are excluded. Zero delta rows = a zero-valued heartbeat PSR (SCRUM-55, section below) so the consumer can tell "no movement" from "PRG dead".
- Resend run (`resend=true`, non-identifying): all current reportable-status rows, watermark ignored; re-projects CURRENT state, not the original report (A-8).
- R-38 exclusion visibility: mid-DAG rows with `status IS NULL` are never reportable. Up to 100 each gets a WARN in the uniform shape `excluded stage=PRG arrival=<uuid> seq=<n> e2e=<e2e> reason=STATUS_UNKNOWN`; above 100 they collapse to one summary WARN per client.
- File: `<exchange-root>/<client-base>/onhost-resp/out/<CLIENT>_PSR_<window>.txt`; layout is SYNTHETIC-CONTRACT (R-35): header `PSR|client|window`, one `TX|e2e|status` per row ordered by e2e, trailer `END|count`. An unconfigured client fails the job closed (`IllegalArgumentException` from the layout) rather than writing to a wrong directory.
- Outcome seam: on COMPLETED, `BUSINESS_ACCEPTED` to `<exchange-root>/outcomes/<JOB_NAME>` (`OutcomeFileWriter`, R-33).

### SYNTHETIC-CONTRACT: heartbeat layout (SCRUM-55)

A SCHEDULED window with zero delta emits a zero-valued, normal-format PSR instead of no file, so the OnHost consumer can distinguish "no movement" from "PRG dead":

```text
PSR|<client>|<window>
HB|DCRE00000000000000000000000000000|DCRE00000000000000000000000000000|0|0.00
PD|<pendingCount>
END|0
```

- The HB placeholder is `DCRE` + 29 zeros (33 chars, Max35-safe). **SYNTHETIC-CONTRACT: the real legacy Payment Report response copybook and its exact zero-placeholder bytes are unrecovered; this shape is invented and is a critical future update once the copybook is attested (design-register A-item, A-57).**
- `PD|<pendingCount>` counts the client's `prg_sla_pending` rows (members of VISIBLE outbound batches whose current status is non-terminal); zero pending prints `PD|0`. HB/PD are not TX lines, so the trailer stays `END|0`.
- A heartbeat registers a `prg_report` row (type `HEARTBEAT`) but NEVER touches the delivery ledger or the watermark: nothing was externally reported. Only the SCHEDULED path heartbeats; IMMEDIATE/MANUAL no-ops stay file-less.

### Database and batch metadata

Liquibase with per-service history tables (`prg_databasechangelog` / `prg_databasechangeloglock`) on the shared `dcre_collections` DB:

1. `2026/07/001-prg.xml` changeSet 001: BOOTSTRAP-ORDER GUARD. PRG is clock-launched and may run on a fresh DB before CRR/CTV and the response readers, so every view source (`tx_header`, `tx_entry`, `validation_log`, `isr_resp`, `sbsr_resp`, `pbsr_resp`) is created `IF NOT EXISTS` with the owners' exact column sets.
2. ChangeSet 002: `ext_tx_status` view, deepest response leg wins (R-17 stage rank PBSR 4 > SBSR 3 > ISR 2 > CTV 1); a PASS validation with no response yet projects as `CTV_PASS`, a FAIL projects its outcome verbatim; client = `tx_header.client_token`.
3. `2026/07/003-reporting.xml`: split-response correlation views, `prg_status_class`, report registry, delivery ledger, due/SLA views and the batch-scoped `ext_tx_status` replacement.
4. `2026/07/004-status-classification.xml`: five-way classification for all fourteen recognised statuses, historical unsupported rows for `ACWC`/`ACWP` (corrected by 005), automatic-report suppression and `prg_status_exception`.
5. `2026/07/005-warehoused-interim.xml`: SCRUM-68 reclassifies `ACWC`/`ACWP` as accepted warehoused interim (reportable, `sla_suppressed`) per the RMB DebiCheck profile and re-owns `prg_sla_pending` with the suppression predicate (later-owner pattern).
6. `2026/07/002-batch-metadata.xml` -> `batch-metadata-prg.sql`: Spring Batch metadata under prefix `PRG_BATCH_` (`spring.batch.jdbc.initialize-schema: never`).

### Platform library dependencies (mavenLocal, 0.1.0)

| Module | Used for |
|---|---|
| `za.co.fnb.dcre:platform-persistence` | `BaseEntity` (on `PrgWatermarkEntity`), `JdbcConfig` (imported by `PrgApplication`) |
| `za.co.fnb.dcre:platform-files` | `ExchangeLayout` / `ExchangeChannel` / `ExchangeSub` (per-client exchange resolution, fail-closed) |
| `za.co.fnb.dcre:platform-batch` | `ExitCodeMain`, `OutcomeFileWriter`, `StaleExecutionSweeper`; ships the `dcre-exchange-layout.yml` classpath resource imported via `spring.config.import` |

## Prerequisites

- Java 25 (Gradle toolchain; Gradle 9.5.1 wrapper included)
- Docker (Testcontainers CockroachDB in tests, image build)
- Platform libs published to Maven Local: run `./gradlew publishToMavenLocal` in `dcre-platform-model` -> `dcre-platform-files` -> `dcre-platform-batch` (each `api`-exposes the previous) and in `dcre-platform-persistence` (standalone)
- A reachable CockroachDB and exchange directory for a real run (the `dcre-infra` kind cluster locally)

## Quickstart

Clean clone, no `.env` needed (committed dev defaults):

```bash
./gradlew build

# one scheduled window against a reachable CockroachDB (defaults: localhost:26257/dcre_collections)
java -jar build/libs/prg-2.0.jar client=FNBRF01 window=w1

# resend override: non-identifying parameter, re-emits all current rows
java -jar build/libs/prg-2.0.jar client=FNBRF01 window=w1-manual resend=true,java.lang.String,false
```

## Configuration

Precedence: yml default < environment variable. All defaults are committed in `application.yml`.

| Env | Default | Purpose |
|---|---|---|
| `DCRE_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_collections?sslmode=disable` | CockroachDB via pgwire |
| `DCRE_DB_USER` | `root` | DB user |
| `DCRE_DB_PASSWORD` | (empty) | DB password |
| `DCRE_EXCHANGE_ROOT` | `../../../../../infra/dcre-infra/exchange` | Exchange root: PSR output tree + outcome seam |
| `DCRE_PRG_PSR_SLICE_SIZE` | `50000` | Keyset slice size for reads, streaming emission and watermark advances |
| `DCRE_AMOUNT_SCALE` | `2` | Fleet-wide flag; not read by PRG sources |
| `DCRE_V1_ENABLED` | `false` | Fleet-wide flag; not read by PRG sources |
| `DCRE_FLOW_DC` | `true` | Fleet-wide flag; not read by PRG sources |
| `JOB_NAME` | `local-<executionId>` | K8s-injected identity for the outcome seam |

Per-client exchange directories bind from the `dcre-exchange-layout.yml` classpath resource (shipped in `platform-batch`, imported via `spring.config.import`); Batch metadata uses table prefix `PRG_BATCH_`; Liquibase history lives in `prg_databasechangelog`(+lock).

## Testing

```bash
./gradlew test
```

Testcontainers CockroachDB `cockroachdb/cockroach:v26.2.3` (Docker required):

- `PrgJobTest`: end-to-end window sequence (first delta with deepest-leg statuses + exactly one R-38 WARN, unchanged window emits the zero-valued heartbeat, single status flip emits exactly that row, resend re-emits all current rows) plus the SCRUM-42 fail-closed unconfigured-client case.
- `PsrReportServiceSliceTest`: bounded-scan proofs (multi-slice delta -> one correct streamed PSR, restart-with-existing-target still advances watermarks, summary WARN above the detail limit).
- `PsrReportServiceRetryTest`: 40001 retry semantics of the watermark advance.
- `ReportingSchemaIT` / `ImmediateReportIT`: SCRUM-55 status classes, delivery-ledger guard, batch-scoped `ext_tx_status` v2, due/SLA views; IMMEDIATE/MANUAL report modes with kill-resume proofs.
- `HeartbeatIT`: SCRUM-55 zero-valued heartbeat (exact file shape, `prg_sla_pending` PD count, HEARTBEAT registry row, watermark/ledger untouched, R-24 restart no-op).
- Cucumber BDD suite: `src/test/resources/features/psr-window-projection.feature` (delta projection, quiet-window heartbeat, status flip, resend, R-38 exclusion, watermark advance).

## Local cluster deployment

```bash
./gradlew bootJar
docker build -t dcre-prg:TAG .
kind load docker-image --name dcre-dev dcre-prg:TAG
```

The image base is `eclipse-temurin:25-jre-alpine`. In the cluster, AGT's `PrgScheduler` ticks against its clock (`AGT_PRG_INTERVAL_SECONDS`, default 60), derives the window counter from the epoch and launches the image from `AGT_PRG_IMAGE` as an ephemeral K8s Job per (client, window) with parameters `client=<client> window=w<n>` and `JOB_NAME` injected; the JobRepository dedupes on the identifying pair (restart-not-duplicate, R-16). An on-demand run is triggered by dropping a `chaos/run-prg-<client>` file under the exchange root (launches a distinct `-manual` window with `resend=true`). Fleet version switching: `dcre-infra` `scripts/switch-version.sh`; cluster bring-up: `scripts/kind-up.sh` (kind cluster `dcre-dev`); clean slate: `scripts/env-reset.sh`.

Releases are digits-only 3-component SemVer git tags, uniform across the fleet (current: 2.1.1).

## Related repositories

- Orchestrator: [dcre-agt](https://github.com/sean-huni/dcre-agt)
- Stage services: [dcre-crr](https://github.com/sean-huni/dcre-crr), [dcre-ctv](https://github.com/sean-huni/dcre-ctv), [dcre-cde](https://github.com/sean-huni/dcre-cde), [dcre-cir](https://github.com/sean-huni/dcre-cir), [dcre-crw](https://github.com/sean-huni/dcre-crw), [dcre-ixr](https://github.com/sean-huni/dcre-ixr), [dcre-sxr](https://github.com/sean-huni/dcre-sxr), [dcre-pxr](https://github.com/sean-huni/dcre-pxr), [dcre-ais](https://github.com/sean-huni/dcre-ais), [dcre-hcs](https://github.com/sean-huni/dcre-hcs)
- Platform libs: [dcre-platform-model](https://github.com/sean-huni/dcre-platform-model), [dcre-platform-files](https://github.com/sean-huni/dcre-platform-files), [dcre-platform-batch](https://github.com/sean-huni/dcre-platform-batch), [dcre-platform-persistence](https://github.com/sean-huni/dcre-platform-persistence)
- Infra and tooling: [dcre-infra](https://github.com/sean-huni/dcre-infra), [dcre-fixture-toolkit](https://github.com/sean-huni/dcre-fixture-toolkit), [dcre-design-register](https://github.com/sean-huni/dcre-design-register), [dcre-rpt](https://github.com/sean-huni/dcre-rpt)
