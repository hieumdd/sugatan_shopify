from datetime import datetime
from typing import Callable, Optional

from google.cloud import bigquery

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

TimeRangeGetter = Callable[
    [
        bigquery.Client,
        str,
        Optional[str],
        Optional[str],
    ],
    tuple[str, str],
]

Loader = Callable[[bigquery.Client, str, list[dict]], int]


def get_time_range(table: str) -> TimeRangeGetter:
    def get(
        client: bigquery.Client,
        dataset: str,
        start: str = None,
        end: str = None,
    ) -> tuple[str, str]:
        if start and end:
            _start, _end = start, end
        else:
            rows = client.query(
                f"""SELECT MAX(updated_at) AS incre FROM {dataset}.{table}"""
            ).result()
            row = [dict(row) for row in rows][0]
            _start = row["incre"].replace(tzinfo=None).strftime(TIMESTAMP_FORMAT)
            _end = datetime.utcnow().strftime(TIMESTAMP_FORMAT)
        return _start, _end

    return get


def load(
    table: str,
    schema: list[dict],
) -> Loader:
    def _load(
        client: bigquery.Client,
        dataset: str,
        rows: list[dict],
    ) -> int:
        output_rows = (
            client.load_table_from_json(
                rows,
                f"{dataset}.{table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND",
                    schema=schema,
                ),
            )
            .result()
            .output_rows
        )
        update(client, dataset, table)
        return output_rows

    return _load


def update(client: bigquery.Client, dataset: str, table: str) -> None:
    client.query(
        f"""
        CREATE OR REPLACE TABLE {dataset}.{table} AS
        SELECT * EXCEPT (row_num) FROM
        (
            SELECT
                *,
                ROW_NUMBER() over
                    (PARTITION BY id ORDER BY updated_at DESC) AS row_num
            FROM
                {dataset}.{table}
        ) WHERE row_num = 1
    """
    ).result()
