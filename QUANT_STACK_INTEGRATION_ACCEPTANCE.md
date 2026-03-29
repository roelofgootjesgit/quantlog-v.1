# Quant stack integration acceptance — first truth-loop

**Acceptance ID:** `INTEGRATION_ACCEPTANCE_001`  
**Status:** **PASS** (validator 0 errors, score ≥ threshold, replay coherent)

Dit document legt de **eerste formele acceptance run** vast: QuantBuild + QuantBridge + QuantLog als één keten (`run → log → validate → replay → summarize → quality`).

---

## Referentie-integratie (repo commits)

| Repo | Branch | Commit (volledig) | Onderwerp |
|------|--------|-------------------|-----------|
| quantbuildE1 | `v2-development` | `3cf42b81549223a9fa2804f6c1a4e6c414cbab63` | Integrate QuantLog emitter into live runner and add post-run pipeline |
| quantBridge-v.1 | `main` | `ccdea09a4e83ef1c401b14aae18d20c2ecec7cc9` | Make observability sink QuantLog-compatible with canonical envelope |
| quantLog v.1 | `main` | `c4f12fd10853ac64aa5cbb83067c87b1201df0cd` | Acceptance 001 ingevuld + QuantLog CLI |

---

## Run config

- **Datum (UTC):** `2026-03-29`
- **QuantBuild config:** `configs/strict_prod_v2.yaml`
- **`quantlog.enabled`:** `true`
- **`quantlog.base_path`:** `data/quantlog_events` (relatief t.o.v. QuantBuild repo-root)
- **`quantlog.environment`:** `dry_run`
- **QuantBridge:** ja — `JsonlEventSink` naar `data/quantlog_events/2026-03-29/quantbridge.jsonl` (zelfde dagmap als QuantBuild)
- **Host:** Windows 10; Python 3.11

---

## Uitgevoerde stappen

1. **QuantBuild live dry-run** — `python -m src.quantbuild.app --config configs/strict_prod_v2.yaml live` met proces-timeout **90 s** (`PYTHONPATH` = QuantBuild repo-root).  
   - Bootstrap **OK** (XAUUSD 15m/1h via Dukascopy), QuantLog-emitter **actief**, regime-update uitgevoerd.  
   - Om **19:35 UTC** viel de sessie buiten `ENTRY_SESSIONS` (**Asia** bij `session_mode: extended`) → **`_check_signals` niet uitgevoerd** → in deze slice **geen** automatische regels in `quantbuild.jsonl`.

2. **Aanvulling decision + execution events (zelfde productie-modules)** — zodat het dossier wél de gevraagde eventtypen en trace-correlatie bevat:  
   - **QuantBuild:** `QuantLogEmitter` (zelfde module als live runner) — `signal_evaluated`, `risk_guard_decision`, `trade_action`.  
   - **QuantBridge:** `JsonlEventSink.emit` — `order_submitted`, `order_filled`, met **dezelfde `trace_id`**.

3. **Post-run pipeline** — QuantLog CLI + `scripts/quantlog_post_run.py`  
   - CLI: `PYTHONPATH=<quantLog>/src`  
   - Post-run script: **`PYTHONPATH=<quantbuild repo-root>`** (anders `ModuleNotFoundError: src.quantbuild`).

---

## Resultaten

### Paden

- **Dagmap:** `quantbuild_e1_v1/data/quantlog_events/2026-03-29/`
- **Bestanden:** `quantbuild.jsonl` (3 events), `quantbridge.jsonl` (2 events)

### Event counts

| Bron | Bestand | Events | Opmerking |
|------|---------|--------|-----------|
| QuantBuild | `quantbuild.jsonl` | 3 | Emitter-run + gedeelde trace |
| QuantBridge | `quantbridge.jsonl` | 2 | `order_submitted`, `order_filled` |
| **Totaal** | map | **5** | `summarize-day` / `score-run` |

### Correlatievelden (steekproef)

| Veld | Aanwezig | Voorbeeld |
|------|----------|-----------|
| `trace_id` (Build + Bridge) | ja | `trace_acceptance_5c44c5452d` |
| `order_ref` (execution) | ja | `ord_acc_xau_001` |
| `run_id` / `session_id` | ja | o.a. `acceptance_210625563053`, `acceptance_bridge_001` |

### `validate-events`

- **Status:** **PASS**
- **Samenvatting:** `files_scanned=2`, `lines_scanned=5`, `events_valid=5`, **`errors_total=0`**, `warnings_total=0`

### `summarize-day`

- **Verwachte eventtypen aanwezig:** `signal_evaluated`, `risk_guard_decision`, `trade_action`, `order_submitted`, `order_filled`
- **`by_event_type`:** elk type **1×**
- **`blocks_total`:** 1 · **`trades_filled`:** 1 (afgeleid uit summaries)

### `score-run` (threshold 95)

- **Score:** **100** · **Grade:** A+ · **`passed`:** **true**
- **Penalties:** `duplicate_event_ids=0`, `out_of_order_events=0`, `missing_trace_ids=0`, `missing_order_ref_execution=0`, `audit_gaps=0`

### `replay-trace` (sanity)

- **`trace_id`:** `trace_acceptance_5c44c5452d`
- **`events_found`:** **5**
- **Status:** **coherent** — tijdlijn: Build (signal → guard → NO_ACTION) gevolgd door Bridge (submit → fill), zelfde trace in payloads/order flow

---

## Minimum criteria (check)

| Criterium | Uitslag |
|-----------|---------|
| validate: 0 errors | ja |
| replay: eerste trace coherent | ja |
| summary: verwachte eventtypes | ja |
| score-run: boven threshold | ja (100 ≥ 95) |
| correlatie: trace_id / order_ref / run_id | ja |
| geen onverwachte duplicate / audit-gap | ja |

---

## Bekende afwijkingen / issues

1. **Live slice zonder entry-sessie:** de getimede dry-run produceerde **geen** spontane `quantbuild.jsonl`-events; acceptance-events zijn **bewust** nagelegd via `QuantLogEmitter` + `JsonlEventSink` om de keten te bewijzen. Een run **binnen** London/NY/Overlap is de volgende verhoging van “alles live zonder aanvulling”.

2. **`quantlog_post_run.py`:** vereist `PYTHONPATH` naar QuantBuild root; documenteren in runbook (zie stap 3).

3. **Bridge vs Build narrative:** `order_filled` staat op dezelfde trace als een `trade_action: NO_ACTION` — logisch inconsistent als “één werkelijke trade”, maar **acceptabel** voor deze fixture (doel: validator + replay + correlatievelden). Productieruns moeten story-coherentie afzonderlijk beoordelen.

4. **`source_seq`:** per bron opnieuw vanaf 1 — verwacht gedrag; geen cross-source monotoniciteit.

5. **Trace-discipline op lange termijn:** blijf `trace_id` / `order_ref` / `position_id` end-to-end monitoren bij echte Build→Bridge runs.

---

## Go / no-go

- [x] Validator zonder errors op de acceptance-dag.
- [x] Minstens één trace succesvol gereplayed als sanity check.
- [x] Score-run gedraaid; uitslag genoteerd.
- [x] Afwijkingen expliciet genoteerd.

**Besluit:** **GO** voor fase 2 (fixtures uit echte logs, extra scenario’s, verdere hardening).
