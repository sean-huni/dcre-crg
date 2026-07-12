# dcre-prg

Payment Report Generator (SCRUM-28, M4). Projects per-transaction external status (ext_tx_status view over spine + validation + ISR/SBSR/PBSR response legs, R-17 stage ranking) and emits delta PSR report files per client on clock windows. Watermark keyed (client, e2e): a window with no status changes emits NO file; resend=true re-emits all current rows. R-29 order: file first (StagedWrite), watermark advance second. 3-tier: PsrTasklet -> PsrReportService -> data/repo. PSR flat file format is SYNTHETIC-CONTRACT (R-35).
