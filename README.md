# dcre-crg

> Part of the DCRE fleet. For the fleet map, the rulings and the diagrams that specify every stage, start at the [DCRE design register](https://github.com/sean-huni/dcre-design-register); the complete list of live repositories is its [Repositories](https://github.com/sean-huni/dcre-design-register#repositories) table.

PSR Generator: clock-windowed status reporter, the terminal stage of the DCRE response leg.

## What it does

| | |
|---|---|
| Stage | `CRG` |
| Family / leg | Collections (DC), RES |
| Trigger | clock-launched: AGT `CrgScheduler` (every 10s, window length `AGT_CRG_INTERVAL_SECONDS`, default 60) launches one SCHEDULED Job per (collections client, window). Also arrival-scoped: AGT `ReportTrigger` launches an IMMEDIATE run per due parent in `prg_report_due` |
| Upstream | none in the DAG. Reads what `CRR`, `CTV`, `CRW`, `CIX`, `CSX` and `CPX` wrote |
| Downstream | none (terminal); OnHost collects the PSR from `onhost-resp/out` |
| Diagram sheet | `dcre-collections-res` |

Position per AGT `CrgScheduler`, `ReportTrigger` and `ReportWindows` on origin/dev (checked 2026-09-28); CRG is not in any `RouteDags` shape. CRG projects per-transaction external status (the `ext_tx_status` view over spine + validation + ISR/SBSR/PBSR response legs, deepest leg wins per R-17) and emits delta Payment Status Report (PSR) files per client on clock windows. Each run diffs `ext_tx_status` against the `prg_watermark` table for one client, streams the delta as a PSR flat file into the client's `onhost-resp/out` exchange directory, then advances the watermark in bounded slices. It is not file-triggered: AGT instantiates `crgJob` per (client, window) as a short-lived Kubernetes Job, and CRG reports whatever `ext_tx_status` holds behind the watermark.

**CRG serves the DC Collections flow only.** It reads and writes `dcre_col` and nothing else. The
ENDO Payments flow gets its own report generator, `prg`, in the payments family against `dcre_pay`
(SCRUM-107). The single-service-for-both-flows arrangement this repo carried until SCRUM-107 was
the SOLID violation the family split exists to remove; the collections/payments sheets in the design register are the specification.

### SCRUM-107: renamed from `prg`; table names stay, the migration history moves

This repository was `dcre-prg` (see `dcre-prg-legacy` for the archive). The rename covers the CODE,
the package, the artifact, the config prefix, the changeset identities, the Spring Batch table
prefix and the Liquibase history tables.

It does **not** rename the DATA tables. `prg_watermark`, `prg_report`, `prg_delivery_ledger`,
`prg_status_class` and the `prg_*` views keep their names, for one reason: `shared/rpt` reads
`prg_report` and `prg_watermark` heavily and is owned elsewhere. Renaming them is a separate change
that needs the rpt owner, and it is recorded as a follow-up rather than carried silently. Class
names follow the SERVICE (`CrgReportEntity`) while `@Table` values follow the SCHEMA
(`@Table("prg_report")`), per the estate rule that a `@Table` value never changes with a class
rename.

The Liquibase history tables DO rename, to `crg_databasechangelog`(+lock). An earlier revision kept
them as `prg_*` under a `[!CONVENTION-OVERRIDE]`, because a history table IS the migration state
and renaming it would have presented an empty history to a fully built `dcre_col` and replayed all
53 changesets. That obstacle is gone: every DCRE database is dropped and recreated at the v1
cutover, so there is no history to preserve and no replay to survive. The override was retired with
the condition that produced it.

One COLUMN is renamed with the v1 baseline: `prg_report.report_type` becomes `prg_report.type`. A
column never repeats its own host table's name, and a baseline on an empty database is the only
moment the correction is free. The payments sibling already ships the corrected spelling.

The Spring Batch prefix is `CRG_BATCH_`.

## Architecture and principles

- **SOLID, 3-tier**: one responsibility per class along `PsrTasklet` (thin Spring Batch entry adapter) -> `PsrReportService` (business tier) -> `CrgWatermarkRepo` (Spring Data JDBC). Supporting single-purpose units: `ImmediateReportService` (IMMEDIATE and MANUAL per-parent reports and ledger replay), `StreamedPsrWrite` (staged streaming boundary write), `CrdbRetry` (bounded SQLSTATE 40001 retry), `CrgReportRepo` / `CrgDeliveryLedgerRepo` (report registry and delivery ledger); platform-batch's `OutcomeSeamListener` and `HeartbeatWriter` are registered on the job. Layer-first packages: `config/`, `service/`, `data/model/`, `data/repo/`.
- **12FactorApp Alignment - https://12factor.net/**: config strictly from the environment with committed working dev defaults (a clean clone runs with no `.env`), stateless one-shot process (the JVM exit code carries the Batch verdict via `ExitCodeMain`, R-34), CockroachDB and the exchange directory as attached resources.
- **Idempotent restart semantics**: job identity is the identifying parameter pair (client, window) (R-16); `resend` is non-identifying. R-29 order: the WHOLE file becomes visible first (streamed tmp + `ATOMIC_MOVE`), then watermarks advance in per-slice `REQUIRES_NEW` transactions. An existing target file is a restart no-op (R-24); a crash between file and watermark replays as skip-existing-file + watermark advance, neither skipping nor duplicating. `StaleExecutionSweeper.abandonStale(ds, "CRG_BATCH_", 60)` runs as an `@Order(-10)` `ApplicationRunner` so a killed pod never strands a STARTED execution (A-39a).
- **CRDB-correct upserts**: watermark writes are `INSERT ... ON CONFLICT (client, e2e) DO UPDATE`, never `UPSERT INTO` (CRDB arbitrates UPSERT on the primary key only; the business identity is (client, e2e)). Serialization aborts (40001) retry up to 5 attempts with jittered backoff in a fresh transaction per attempt.
- **Isolation**: the Hikari pool runs `TRANSACTION_READ_COMMITTED` (`spring.datasource.hikari.transaction-isolation`).
- **Bounded scale (SCRUM-42)**: whole-book reads plus a full in-heap render blew CRDB's sql memory budget on the 30M-tx book. Every read is now a keyset slice (`ORDER BY e2e LIMIT :limit`, default 50000) and the PSR streams to disk slice by slice; nothing holds more than one slice in heap.

### Fintegrate status classification

`prg_status_class` is the runtime authority. All fourteen recognised codes are explicit:

- Terminal success: `ACSC`, `ACCC`.
- Terminal non-success: `RJCT`, `CANC`.
- Accepted non-terminal: `ACSP`, `ACTC`, `ACCP`, `ACFC`.
- Pending/interim: `RCVD`, `PDNG`, `PART`, `PATC`.
- Accepted warehoused (SLA-suppressed): `ACWP` future-dated, `ACWC` auto-bumped per the RMB
  DebiCheck profile.

Accepted warehoused codes (SCRUM-68) are reportable non-terminal
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

One job `crgJob`, one tasklet step `psrStep`. `PsrTasklet` dispatches on the non-identifying `report.type` (default `SCHEDULED`; `IMMEDIATE`; `MANUAL`; anything else fails the job):

- `SCHEDULED`: `client`, `window`, optional `resend`.
- `IMMEDIATE`: `client`, `window`, `parents` (required; one source MsgId per AGT launch, comma-joined here yields one file per parent).
- `MANUAL`: `report.id` replays a delivered report from the ledger; otherwise `parents` plus optional `manual.ref`.

- Scheduled run: delta selection, reportable rows whose `ext_tx_status.status` moved past `prg_watermark.last_status` (or have no watermark row yet). Unknown Fintegrate response codes are excluded. Zero delta rows = a zero-valued heartbeat PSR (SCRUM-55, section below) so the consumer can tell "no movement" from "CRG dead".
- Resend run (`resend=true`, non-identifying): all current reportable-status rows, watermark ignored; re-projects CURRENT state, not the original report (A-8).
- R-38 exclusion visibility: mid-DAG rows with `status IS NULL` are never reportable. Up to 100 each gets a WARN in the uniform shape `excluded stage=CRG arrival=<uuid> seq=<n> e2e=<e2e> reason=STATUS_UNKNOWN`; above 100 they collapse to one summary WARN per client.
- File: `<exchange-root>/<client-base>/onhost-resp/out/<CLIENT>_PSR_<window>.txt`; layout is SYNTHETIC-CONTRACT (R-35): header `PSR|client|window`, one `TX|e2e|status` per row ordered by e2e, trailer `END|count`. An unconfigured client fails the job closed (`IllegalArgumentException` from the layout) rather than writing to a wrong directory.
- Outcome seam: on COMPLETED, platform-batch's `OutcomeSeamListener` writes `BUSINESS_ACCEPTED` to `<exchange-root>/outcomes/<JOB_NAME>` (R-33). The resolved job name is also stored on every `prg_report` row.

### SYNTHETIC-CONTRACT: heartbeat layout (SCRUM-55)

A SCHEDULED window with zero delta emits a zero-valued, normal-format PSR instead of no file, so the OnHost consumer can distinguish "no movement" from "CRG dead":

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

| Datasource | Database (dev default) | Env vars | Access |
|---|---|---|---|
| primary | `dcre_col` | `DCRE_DB_URL`, `DCRE_DB_USER`, `DCRE_DB_PASSWORD` | read/write |
| heartbeat (platform-batch) | `agt_ops` | `DCRE_AGTOPS_DB_URL`, `DCRE_AGTOPS_DB_USER`, `DCRE_AGTOPS_DB_PASSWORD` | `HeartbeatWriter` liveness stamp |

Writes: `prg_watermark`, `prg_report`, `prg_delivery_ledger` (and `prg_status_class` seed rows via Liquibase). Reads: `tx_header`, `tx_entry` (CRR), `validation_log` (CTV), `isr_resp` / `sbsr_resp` / `pbsr_resp` (CIX / CSX / CPX), `crw_emission_group` / `crw_emission` / `crw_emission_member` (CRW), through its own views. Views it publishes for other readers: `prg_report_due` and `prg_sla_pending` (read by AGT's read-only collections datasource), and `man_collection_outcome` (read by mandates MRG over a second datasource, a `[!CONVENTION-OVERRIDE]` recorded in the changelog).

Liquibase with per-service history tables (`crg_databasechangelog` / `crg_databasechangeloglock`)
on the shared `dcre_col` database. The changelog is a VERSION 1 BASELINE: the pre-v1 changelogs
under `2026/07` are gone rather than superseded, because the database is dropped and recreated at
the cutover, so there is no history for them to be consistent with. The root master includes the
MONTH sub-master, never individual changesets.

1. `2026/08/001-batch-metadata.xml`: Spring Batch 6.0.4 job-repository metadata under prefix
   `CRG_BATCH_`, pure typed tags (no external SQL file, so no ANY-checksum override).
2. `2026/08/002-reporting-tables.xml`: the tables CRG OWNS (`prg_watermark`, `prg_report`,
   `prg_delivery_ledger`, the `uq_ledger_auto` partial unique guard) plus the CRG-owned `ix_prg_*`
   read indexes.
3. `2026/08/003-status-classification.xml`: `prg_status_class` and all fourteen recognised codes,
   stated once in their settled form.
4. `2026/08/004-reporting-views.xml`: the pick views, `ext_tx_status`, `prg_member_status`,
   `prg_report_due`, `prg_sla_pending` and `prg_status_exception`, each defined exactly once.
5. `2026/08/005-man-collection-outcome.xml`: `man_collection_outcome` (A-70, M10), terminal-failure/success/pending per mandate, for MRG's suspension sweep.

**CRG no longer creates its read sources, and that is an ordering contract.** The pre-v1 changelog
bootstrap-minted nine relations CRG does not own, so a clock-launched window could run before the
writers had ever executed: `tx_header` and `tx_entry` (CRR), `validation_log` (CTV), `isr_resp` /
`sbsr_resp` / `pbsr_resp` (CIX / CSX / CPX), and `crw_emission_group` / `crw_emission` /
`crw_emission_member` (CRW). At v1 the owners create those tables unguarded, so a CRG mint that won
the race would crashloop the owner on "relation already exists"; the mint had also already drifted,
missing CRR's `content_hash` column and its index. CRG's migration must therefore run AFTER the
writers'. A CREATE VIEW and a CREATE INDEX both resolve their target at creation time, so running
too early now fails immediately and names the missing relation instead of inventing one.

Integration tests stand the read sources up from `src/test/resources/db/changelog/test/001-read-sources.xml`,
reached through `db.changelog-test-master.xml`, which then runs the production master unchanged.

### Platform library dependencies (mavenLocal, 0.1.0, declared in `build.gradle`)

| Module | Used for |
|---|---|
| `za.co.fnb.dcre:platform-persistence` | `BaseEntity` (on `CrgWatermarkEntity`), `JdbcConfig` (imported by `CrgApplication`) |
| `za.co.fnb.dcre:platform-files` | `ExchangeLayout` / `ExchangeChannel` / `ExchangeSub` (per-client exchange resolution, fail-closed) |
| `za.co.fnb.dcre:platform-batch` | `ExitCodeMain`, `BatchJdbcConfig`, `HeartbeatDatasourceConfig`, `HeartbeatWriter`, `OutcomeSeamListener`, `OutcomeFileWriter.jobNameOrLocal`, `StaleExecutionSweeper`; ships the `dcre-exchange-layout.yml` classpath resource imported via `spring.config.import` |

## Prerequisites

- Java 25 (`.sdkmanrc`: `java=25-tem`; `build.gradle` sets source/target compatibility 25)
- Gradle 9.5.1 via the wrapper
- Docker (Testcontainers CockroachDB in tests, image build)
- Platform libs published to Maven Local: run `./gradlew publishToMavenLocal` in `dcre-platform-model` -> `dcre-platform-files` -> `dcre-platform-batch` (each `api`-exposes the previous) and in `dcre-platform-persistence` (standalone)
- A reachable CockroachDB and exchange directory for a real run (the `dcre-infra` kind cluster locally)

## Quickstart

Clean clone, no `.env` needed (committed dev defaults):

```bash
./gradlew build

# one scheduled window against a reachable CockroachDB (defaults: localhost:26257/dcre_col)
java -jar build/libs/crg-2.0.jar client=FNBRF01 window=w1

# resend override: non-identifying parameter, re-emits all current rows
java -jar build/libs/crg-2.0.jar client=FNBRF01 window=w1-manual resend=true,java.lang.String,false

# immediate report for one parent (what AGT's ReportTrigger launches)
java -jar build/libs/crg-2.0.jar client=FNBRF01 window=<key> \
  report.type=IMMEDIATE,java.lang.String,false 'parents=<sourceMsgId>,java.lang.String,false'
```

## Configuration

Precedence: yml default < environment variable. All defaults are committed in `application.yml`.

| Env | Default | Purpose |
|---|---|---|
| `DCRE_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_col?sslmode=disable` | CockroachDB via pgwire |
| `DCRE_DB_USER` | `root` | DB user |
| `DCRE_DB_PASSWORD` | (empty) | DB password |
| `DCRE_AGTOPS_DB_URL` | `jdbc:postgresql://localhost:26257/agt_ops?sslmode=disable` | heartbeat datasource |
| `DCRE_AGTOPS_DB_USER` / `DCRE_AGTOPS_DB_PASSWORD` | `root` / (empty) | heartbeat credentials |
| `DCRE_EXCHANGE_ROOT` | `../../../../../../infra/dcre-infra/exchange` | Exchange root: PSR output tree + outcome seam |
| `DCRE_CRG_PSR_SLICE_SIZE` | `50000` | Keyset slice size for reads, streaming emission and watermark advances |
| `DCRE_AMOUNT_SCALE` | `2` | Fleet-wide flag; not read by CRG sources |
| `DCRE_V1_ENABLED` | `false` | Fleet-wide flag; not read by CRG sources |
| `JOB_NAME` | `local-crg-<executionId>` | K8s-injected identity for the outcome seam and `prg_report` rows |

This is the documented set, not a closed total: Spring relaxed binding lets any Spring or `dcre.*` property be overridden by its derived environment variable name.

Per-client exchange directories bind from the `dcre-exchange-layout.yml` classpath resource (shipped in `platform-batch`, imported via `spring.config.import`); Batch metadata uses table prefix `CRG_BATCH_`; Liquibase history lives in `crg_databasechangelog`(+lock).

## Testing

```bash
./gradlew test
```

Testcontainers CockroachDB `cockroachdb/cockroach:v26.2.3` (Docker required):

- `CrgJobTest`: end-to-end window sequence (first delta with deepest-leg statuses + exactly one R-38 WARN, unchanged window emits the zero-valued heartbeat, single status flip emits exactly that row, resend re-emits all current rows) plus the SCRUM-42 fail-closed unconfigured-client case.
- `PsrReportServiceSliceTest`: bounded-scan proofs (multi-slice delta -> one correct streamed PSR, restart-with-existing-target still advances watermarks, summary WARN above the detail limit).
- `PsrReportServiceRetryTest`: 40001 retry semantics of the watermark advance.
- `ReportingSchemaIT` / `ImmediateReportIT`: SCRUM-55 status classes, delivery-ledger guard, batch-scoped `ext_tx_status` v2, due/SLA views; IMMEDIATE/MANUAL report modes with kill-resume proofs.
- `HeartbeatIT`: SCRUM-55 zero-valued heartbeat (exact file shape, `prg_sla_pending` PD count, HEARTBEAT registry row, watermark/ledger untouched, R-24 restart no-op).
- `JobNameCaptureIT`: scheduled, heartbeat and immediate reports persist a non-null job name; kill-resume leaves one row per report identity.
- `PsrReportServiceAdvanceHonestyTest`: a status moving between stream and advance is ledgered at the emitted status.
- `ClientAuthorityIT`: every projection names one client under the ruled client authority.
- `ManCollectionOutcomeIT`: `man_collection_outcome` carries the columns MRG reads and classifies per mandate.
- `ReportDuePerfIT`: `prg_report_due` and a full `ext_tx_status` scan answer within five seconds on a 12,000-tx parent.
- `ConfigPlaceholderBindingTest`: every `dcre.*` placeholder in main sources resolves against the committed yaml; the retired `prg` prefix is gone.
- Read sources for the integration tests come from `src/test/resources/db/changelog/test/001-read-sources.xml` via `db.changelog-test-master.xml`.
- Cucumber BDD suite: `src/test/resources/features/psr-window-projection.feature` (delta projection, quiet-window heartbeat, status flip, resend, R-38 exclusion, watermark advance).

## Local cluster deployment

```bash
VERSION=<fleet release tag>
./gradlew bootJar
docker build -t dcre-crg:$VERSION .
kind load docker-image --name dcre-dev dcre-crg:$VERSION
kubectl set env -n dcre deploy/dcre-agt AGT_CRG_IMAGE=dcre-crg:$VERSION
```

The image base is `eclipse-temurin:25-jre-alpine`. AGT reads the image from `AGT_CRG_IMAGE` (empty by default, which leaves the stage launch-disabled). dcre-infra `scripts/switch-version.sh` does NOT set `AGT_CRG_IMAGE`: its stage list still exports `AGT_PRG_IMAGE=dcre-prg:<version>`, which AGT now reads for the PAYMENTS generator (checked 2026-09-28), hence the `kubectl set env` above.

In the cluster, AGT (origin/dev, checked 2026-09-28) launches CRG into the collections flow namespace (AGT `AGT_NAMESPACE_COL`, default `dcre-col`) with env `JOB_NAME`, `DCRE_DB_URL` (AGT `service-db-url`, `dcre_col`), `DCRE_EXCHANGE_ROOT=/exchange`, `DCRE_AGTOPS_DB_URL`, `DCRE_AGTOPS_DB_USER`:

- `CrgScheduler`, for clients whose flow is collections: `client=<client> window=w<n>`, the window counter derived from the epoch, so the JobRepository dedupes on the identifying pair (restart-not-duplicate, R-16).
- `ReportTrigger`: one IMMEDIATE run per due parent in `prg_report_due`, `client`, a digest `window` stable per (client, parent), `report.type=IMMEDIATE`, `parents=<sourceMsgId>`.
- On demand: dropping `<exchange-root>/chaos/run-crg-<client>` launches a distinct `-manual` window with `resend=true`.

Cluster bring-up: dcre-infra `scripts/kind-up.sh` (kind cluster `dcre-dev`); clean slate: `scripts/env-reset.sh`. Releases are digits-only 3-component SemVer git tags, uniform across the fleet.

## Related repositories

The complete, current list of live DCRE repositories (stage services, orchestrator, platform libraries, infra and tooling) lives in one place: the [DCRE design register README](https://github.com/sean-huni/dcre-design-register#repositories). Deprecated and archived repositories are deliberately absent from it. This README does not copy that list, so it cannot drift.

- Design register: https://github.com/sean-huni/dcre-design-register (start at `docs/specs/DESIGN-REGISTER.md`; the diagrams in `docs/diagrams/` are the specification)
