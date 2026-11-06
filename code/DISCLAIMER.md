# Disclaimer

Plain-language summary. The binding terms are the [Apache License 2.0](LICENSE) (sections 7 and 8) and [`NOTICE`](NOTICE).

## 1. Provided "AS IS"

- ALELT is free, open-source software provided **"AS IS"**, without warranty or condition of any kind, express or implied — including merchantability, fitness for a particular purpose, non-infringement, accuracy or availability.
- No support, maintenance, updates or service levels are promised.

## 2. No liability

The authors, maintainers and contributors accept **no liability** for any damage arising from use or inability to use the software, including:

| Area | Examples |
|---|---|
| Data | loss, corruption, duplication, missing or late changes, wrong values |
| Operations | downtime, performance impact on source or destination systems, failed jobs |
| Costs | cloud, warehouse, storage, egress, licence or staff costs |
| Compliance | privacy, retention, audit, regulatory or contractual obligations |
| Security | exposure caused by configuration, deployment or third-party components |

## 3. You are responsible for

| Responsibility | What it means |
|---|---|
| Testing | Validate every pipeline in a non-production environment first (the local lab and parallel-test mode exist for this). |
| Backups | Keep tested backups of sources, destinations and the hub metadata database. |
| Approvals | Obtain the approvals your organisation requires (change advisory board, DBA, security, data owners) before connecting to any system. |
| Production use | Decide whether, when and how to run it against production; monitor it; keep the production deny list configured. |
| Vendor licences | Obtain and comply with licences for vendor software you install, e.g. the IBM Db2 client, Oracle client libraries, SQL Server, Snowflake or any cloud service. None are included. |
| Data protection | Classify data, configure encryption, access control and retention for your obligations. |

## 4. Independence

- Not affiliated with, endorsed by or sponsored by any database, data platform or cloud vendor.
- Product names are used only to identify the engines a connector works with. Trademarks belong to their owners (see [`NOTICE`](NOTICE)).
- Capture and load use only public, documented vendor APIs — see [`docs/PUBLIC-APIS.md`](docs/PUBLIC-APIS.md).

## 5. No advice

Documentation, runbooks, AI-assisted diagnosis output and examples are information, not professional, legal or compliance advice. AI-generated suggestions are never applied automatically and must be reviewed by a person.
