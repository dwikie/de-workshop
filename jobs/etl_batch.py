
import csv
import io

from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import (
    FileSource,
    FileSink,
    StreamFormat,
)
from pyflink.datastream.formats.parquet import ParquetBulkWriters
from pyflink.table.types import RowType, VarCharType, RowField
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.file_system import OutputFileConfig


SOURCE = "s3://source-data/transaction/transactions.csv"
DESTINATION = "s3://warehouse/batch/transaction/"

COLUMNS = [
    "accountID",
    "transactionID",
    "transactionAmountUSD",
    "transactionCurrencyCode",
    "localHour",
    "transactionScenario",
    "transactionType",
    "transactionIPaddress",
    "ipState",
    "ipPostalCode",
    "ipCountry",
    "isProxyIPNullable",
    "browserLanguage",
    "paymentInstrumentType",
    "cardType",
    "paymentBillingPostalCode",
    "paymentBillingState",
    "paymentBillingCountryCode",
    "shippingPostalCode",
    "shippingState",
    "shippingCountry",
    "cvvVerifyResult",
    "digitalItemCount",
    "physicalItemCount",
    "transactionDateTime",
]

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_parallelism(1)

# Read a bounded file as lines.
source = FileSource.for_record_stream_format(
    StreamFormat.text_line_format(),
    SOURCE,
).build()

lines = env.from_source(
    source,
    watermark_strategy=WatermarkStrategy.no_watermarks(),
    source_name="transaction-csv",
)

# Preserve the CSV values as strings in the initial ingestion.
row_type = RowType([
    RowField(name, VarCharType(65535))
    for name in COLUMNS
])
writer_factory = ParquetBulkWriters.for_row_type(row_type)

output_config = (
    OutputFileConfig.builder()
    .with_part_prefix("part")
    .with_part_suffix(".parquet")
    .build()
)

sink = (
    FileSink.for_bulk_format(DESTINATION, writer_factory)
    .with_output_file_config(output_config)
    .build()
)

def parse_csv_line(line):
    values = next(csv.reader([line]))

    # Skip the header.
    if values and values[0] == "accountID":
        return []

    if len(values) != len(COLUMNS):
        raise ValueError(
            f"Expected {len(COLUMNS)} columns, got {len(values)}"
        )

    return [Row(*values)]

rows = lines.flat_map(
    parse_csv_line,
    output_type=Types.ROW_NAMED(
        COLUMNS,
        [Types.STRING()] * len(COLUMNS),
    ),
)

rows.sink_to(sink)

env.execute("transaction-csv-to-parquet")