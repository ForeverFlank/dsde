"""Ruam Bike: your lakehouse

Fill in q2-q4: q1 is already answered, as an example of how to answer the others. Don't change
the lines marked "DO NOT modify this line": the grader calls these functions by name, with
these arguments. You may add imports and your own helper functions (an `ingest` and a
`transform` to share between q2 and q4, say), and you may write with plain SQL on
`catalog.connection` as well as with the pyducklake API, as long as the tables, the commit
messages and the number of snapshots are right.

How you're graded:
- Each Question is graded on its own, starting from a correct lakehouse: what the official
  solution's earlier Questions leave. A mistake in q2 doesn't cost you points in q3, but on
  your own machine q3 runs after your q2, so fix earlier Questions first.
- Each Question is scored on what it returns and on what it leaves in the lakehouse: the
  tables' schemas and rows, and its snapshots and their commit messages.
- The grader uses other data with the same file names. Hard-coded answers or ids will fail.

Run `uv run python student.py` to rebuild an empty lakehouse next to this file and run
q1 -> q4 on the files in data/.
"""

import hashlib
import pyarrow as pa
import pyarrow.csv as pacsv
import shutil
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from pyducklake import (
    Catalog,
    DateType,
    DecimalType,
    IntegerType,
    Schema,
    StringType,
    TimestampTZType,
    optional,
    required,
)

SOURCE_COLUMNS = [
    "rental_id",
    "member_id",
    "bike_id",
    "start_station",
    "end_station",
    "started_at",
    "ended_at",
    "status",
    "fare_thb",
]

BRONZE_SCHEMA = Schema.of(
    *(optional(name, StringType()) for name in SOURCE_COLUMNS),
    required("source_file", StringType()),
    required("source_row_number", IntegerType()),
    required("file_sha256", StringType()),
    required("ingested_at", TimestampTZType()),
)

SILVER_SCHEMA = Schema.of(
    required("rental_id", StringType()),
    required("member_id", StringType()),
    required("bike_id", StringType()),
    required("start_station", StringType()),
    optional("end_station", StringType()),
    required("status", StringType()),
    required("started_at", TimestampTZType()),
    optional("ended_at", TimestampTZType()),
    optional("fare_thb", DecimalType(8, 2)),
    required("ride_date", DateType()),
)

bronze = None
silver = None

BKK = ZoneInfo("Asia/Bangkok")


def q1(catalog: Catalog):  # DO NOT modify this line
    global bronze, silver
    # Already answered: the namespaces and both tables, in ONE transaction, "q1: create tables".
    with catalog.begin_transaction():
        catalog.set_commit_message("q1: create tables")
        catalog.create_namespace("bronze")
        catalog.create_namespace("silver")
        bronze = catalog.create_table(("bronze", "rentals_raw"), BRONZE_SCHEMA)
        silver = catalog.create_table(("silver", "rentals"), SILVER_SCHEMA)
    return {
        namespace: sorted(name for _, name in catalog.list_tables(namespace))
        for namespace in ("bronze", "silver")
    }


def q2(catalog: Catalog, csv_path: str):  # DO NOT modify this line
    # Part A, ingest into bronze:
    # - file guard: skip the whole file if bronze already has a row with its SHA-256;
    # - row guard: drop a row whose rental_id is already in bronze, or earlier in the same file
    #   (keep the first);
    # - append what is left in one transaction, "q2: bronze ingest rentals_2026-06.csv".
    # Part B, transform into silver ("q2: silver transform"): bronze rows whose rental_id isn't
    # in silver yet (NOT EXISTS), trimmed, typed with TRY_CAST, ride_date in Bangkok time,
    # validated by the handout's rules, one per rental_id (QUALIFY).
    # Return {'bronze_appended': int, 'silver_appended': int, 'rejected': int}.

    """-------------------- Part A --------------------"""

    file_bytes = Path(csv_path).read_bytes()
    file_hash = hashlib.sha256(file_bytes).hexdigest()

    # Guard 1: the exact same file was ingested before
    if bronze.scan(f"file_sha256 = '{file_hash}'").count() > 0:
        print(
            f"SKIP {csv_path.name}: this file (sha256 {file_hash[:12]}...) is already in bronze"
        )
        return 0

    # Read every column as text: bronze keeps source values as they arrived
    raw = pacsv.read_csv(
        csv_path,
        convert_options=pacsv.ConvertOptions(
            column_types={col: pa.string() for col in SOURCE_COLUMNS},
            strings_can_be_null=True,  # blank cell -> NULL
        ),
    )
    if raw.column_names != SOURCE_COLUMNS:
        raise ValueError(f"Unexpected header in {csv_path.name}: {raw.column_names}")

    # Add ingest metadata columns
    n = raw.num_rows
    batch = (
        raw.append_column("source_file", pa.array([csv_path.name] * n, pa.string()))
        .append_column("source_row_number", pa.array(range(1, n + 1), pa.int32()))
        .append_column("file_sha256", pa.array([file_hash] * n, pa.string()))
        .append_column(
            "ingested_at",
            pa.array([datetime.now(BKK)] * n, pa.timestamp("us", tz="Asia/Bangkok")),
        )
    )

    # Guard 2: keep only rental_ids that are not in bronze yet (and the first copy inside this file)
    con = catalog.connection
    con.register("incoming", batch)
    new_rows = con.sql("""
        SELECT *
        FROM incoming AS i
        WHERE NOT EXISTS (
            SELECT 1 FROM lake.bronze.rentals_raw AS b WHERE b.rental_id = i.rental_id
        )
        QUALIFY row_number() OVER (PARTITION BY rental_id ORDER BY source_row_number) = 1
        ORDER BY source_row_number
    """).to_arrow_table()
    con.unregister("incoming")

    # duplicates = n - new_rows.num_rows
    # if new_rows.num_rows == 0:
    #     print(
    #         f"SKIP {csv_path.name}: all {n} rows are duplicates of rows already in bronze"
    #     )
    #     return 0

    with catalog.begin_transaction() as txn:
        catalog.set_commit_message(f"bronze: ingest {csv_path.name}", author="jsudo")
        txn.load_table(("bronze", "rentals_raw")).append(new_rows)

    bronze_appended = new_rows.num_rows

    # print(
    #     f"OK   {csv_path.name}: {n} rows read, {new_rows.num_rows} appended, {duplicates} duplicates skipped"
    # )

    """-------------------- Part B --------------------"""
    TRANSFORM_SQL = """
    WITH new_rows AS (               -- 1. bronze rows that are not in silver yet
        SELECT b.*
        FROM lake.bronze.rentals_raw AS b
        WHERE NOT EXISTS (
            SELECT 1 FROM lake.silver.rentals AS s WHERE s.rental_id = b.rental_id
        )
    ),
    typed AS (                       -- 2. parse text into proper types
        SELECT
            trim(rental_id)                                 AS rental_id,
            trim(member_id)                                 AS member_id,
            trim(bike_id)                                   AS bike_id,
            trim(start_station)                             AS start_station,
            trim(end_station)                               AS end_station,
            trim(status)                                    AS status,
            TRY_CAST(started_at AS TIMESTAMPTZ)             AS started_at,
            TRY_CAST(ended_at AS TIMESTAMPTZ)               AS ended_at,
            TRY_CAST(fare_thb AS DECIMAL(8, 2))             AS fare_thb)
            ingested_at
        FROM new_rows
    ),
    derived AS (                     --    derive date in Bangkok local time
        SELECT
            *,
            CAST(timezone('Asia/Bangkok', requested_at) AS DATE) AS ride_date,
        FROM typed
    )
    SELECT                           -- 3. validate; column order matches silver.rentals
        rental_id, member_id, bike_id, start_station, end_station, status,
        started_at, ended_at,
        fare_thb, ride_date,
    FROM derived
    WHERE rental_id IS NOT NULL
    AND member_id IS NOT NULL
    AND bike_id IS NOT NULL
    AND start_station IN ('Siam', 'Samyan', 'Chamchuri', 'Lumphini', 'Silom')
    AND end_station IN (NULL, 'Siam', 'Samyan', 'Chamchuri', 'Lumphini', 'Silom')
    AND status IS NOT NULL
    AND started_at <= ended_at
    AND (
           (status = 'completed'
            AND picked_up_at IS NOT NULL AND completed_at IS NOT NULL
            AND accepted_at <= picked_up_at AND picked_up_at <= completed_at
            AND fare_thb IS NOT NULL AND fare_thb >= 0
            AND cancelled_at IS NULL)
        OR (status = 'cancelled'
            AND cancelled_at IS NOT NULL AND accepted_at <= cancelled_at
            AND picked_up_at IS NULL AND completed_at IS NULL AND fare_thb IS NULL)
    )
    QUALIFY row_number() OVER (PARTITION BY rental_id ORDER BY ingested_at) = 1   -- one row per rental_id
    ORDER BY requested_at
    """

    # Move new, valid bronze rows into silver.trips. Returns the number of rows appended.
    pending = con.sql("""
        SELECT count(*) AS n FROM lake.bronze.rentals_raw AS b
        WHERE NOT EXISTS (SELECT 1 FROM lake.silver.rentals AS s WHERE s.rental_id = b.rental_id)
    """).df().n[0]
    clean = con.sql(TRANSFORM_SQL).to_arrow_table()

    # if clean.num_rows == 0:
    #     print(
    #         f"Nothing to do: {pending} bronze rows not yet in silver, 0 valid rows to append"
    #     )
    #     return 0

    with catalog.begin_transaction() as txn:
        catalog.set_commit_message(
            f"silver: transform {clean.num_rows} new trips", author="jsudo"
        )
        txn.load_table(("silver", "trips")).append(clean)

    # print(
    #     f"{pending} bronze rows not yet in silver -> {clean.num_rows} appended, {pending - clean.num_rows} rejected by validation"
    # )

    silver_appended = clean.num_rows
    rejected = pending - clean.num_rows

    return {
        "bronze_appended": bronze_appended,
        "silver_appended": silver_appended,
        "rejected": rejected,
    }


def q3(catalog: Catalog):  # DO NOT modify this line
    # Read only: make no snapshot.
    # Return {'silver_rows': int, 'lost_rentals': int,
    #         'revenue_by_station': {station: float, rounded to 2, all 5 Stations},
    #         'rejected_ids': sorted rental_ids in bronze with no silver row}.
    raise NotImplementedError("q3 isn't answered yet")


def q4(  # DO NOT modify this line
    catalog: Catalog, month1_csv: str, month2_csv: str, corrections_csv: str
):
    # Ingest July (month2_csv), then June again (month1_csv: your file guard skips it);
    # transform; apply the Corrections (refares with upsert, voids with delete) in ONE
    # transaction, "q4: silver corrections"; then count silver as it was at the
    # "q2: silver transform" snapshot. See the handout for every key.
    raise NotImplementedError("q4 isn't answered yet")


if __name__ == "__main__":
    here = Path(__file__).resolve().parent
    data = here / "data"
    lakehouse = here / "lakehouse"
    shutil.rmtree(lakehouse, ignore_errors=True)
    lakehouse.mkdir()
    catalog = Catalog(
        "lake",
        str(lakehouse / "lake.ducklake"),
        data_path=str(lakehouse / "data") + "/",
    )
    catalog.set_option("data_inlining_row_limit", "0")
    catalog.connection.execute("SET TimeZone = 'Asia/Bangkok'")
    # Each print below is followed by what it prints on the sample data in data/ when your
    # answer is right. (The grader uses other data, so its values differ.)
    print("q1:", q1(catalog))
    # q1: {'bronze': ['rentals_raw'], 'silver': ['rentals']}
    print("q2:", q2(catalog, str(data / "rentals_2026-06.csv")))
    # q2: {'bronze_appended': 149, 'silver_appended': 141, 'rejected': 8}
    print("q3:", q3(catalog))
    # q3: {'silver_rows': 141, 'lost_rentals': 7,
    #      'revenue_by_station': {'Siam': 600.25, 'Samyan': 1007.25, 'Chamchuri': 778.0,
    #                             'Lumphini': 853.5, 'Silom': 1071.5},
    #      'rejected_ids': ['R0603-017', 'R0605-030', 'R0608-002', 'R0609-011', 'R0609-025',
    #                       'R0611-028', 'R0630-034']}
    print(
        "q4:",
        q4(
            catalog,
            str(data / "rentals_2026-06.csv"),
            str(data / "rentals_2026-07.csv"),
            str(data / "corrections_2026-07.csv"),
        ),
    )
    # q4: {'bronze_appended': 149, 'silver_appended': 141, 'rows_after_q2': 141,
    #      'rows_after_corrections': 279}
