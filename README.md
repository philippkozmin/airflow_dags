# airflow_dags

## dlp_dag_example

Hourly demo refresh in preprod, at minute 0 (`0 * * * *`, Europe/Moscow).
Three sequential tasks: `source_to_ods` -> `ods_to_dds` -> `dds_to_marts`.
Catchup is disabled, runs cannot overlap, and the DAG is initially unpaused.

DAG: `dags/dlp_dag_example.py`. Versioned SQL: `scripts/dlp_dag_example/`.
Each script replaces its target demo table from the preceding layer, preserving
existing cleaning/calculation/aggregation logic without appending duplicate rows.
Catalog: `scale2026-restcatalog`; table in each layer: `dlp_demo_orders`.
Connection: `oqw0dl2e6wpe7`; workbook: `139mu7je0460k`.

| SQL script/task | Saved DLP SQL query |
| --- | --- |
| source_to_ods | 24b8g34xz81il |
| ods_to_dds | 46dai9usofw2n |
| dds_to_marts | uw3082o0uhbqd |

Airflow runs these saved queries; changes to SQL files must also be applied to the
corresponding saved DLP query before deployment. Worker authentication uses the
attached service account via `yandexcloud.SDK()` inside each task; the account
must have access to the workbook/connection in organization
`aatjshkh6qiphjpq10tv`. No desktop credentials are used. The shared `dlp_sdk_preprod.py`
uses the existing worker-network endpoint `https://api.preprod.datalens.tech:20197`.
A non-success SQL response fails the task and prevents downstream execution.

Local validation used Airflow/SDK stubs (Airflow is not installed locally),
plus a real sequential SQL run through DLP MCP with the desktop preprod profile.
Publishing to Git does not by itself verify scheduler import or worker access.

## Production SQL SDK

`dags_prod/dlp_sdk.py` uses `https://api.datalens.tech/rpc/runSqlQuery`
(without an explicit port); `environment="prod"` is the default and only supported
environment. Pass `org_id` explicitly for the target production organization.
The existing preprod DAG imports `dags/dlp_sdk_preprod.py`.
