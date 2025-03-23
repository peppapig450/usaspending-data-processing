import re
from pathlib import Path
from collections.abc import Generator
import pyarrow as pa
import pyarrow.csv as pa_csv
import zstandard as zstd
import logging

logger = logging.getLogger(__name__)
FILENAME_REGEX = re.compile(r"_(\d+)\.csv.zst$")

def empty_generator() -> Generator[pa.RecordBatch, None, None]:
    """Yield an empty generator."""
    yield from ()

def load_zstd_to_batches(
    zstd_path: Path,
    year: str,
    convert_opts: pa_csv.ConvertOptions,
    parse_opts: pa_csv.ParseOptions,
) -> tuple[Generator[pa.RecordBatch, None, None], pa.Schema]:
    """
    Stream zstd file and yield record batches with year and file columns.

    Args:
        zstd_path: Path to the compressed zstd file.
        year: Year to add as a column.
        convert_opts: PyArrow CSV conversion options.
        parse_opts: PyArrow CSV parsing options.

    Returns:
        tuple: (Generator yielding RecordBatches, Schema of the batches).
    """
    zstd_file = zstd_path.open("rb")
    reader = zstd.ZstdDecompressor(max_window_size=2 << 30).stream_reader(zstd_file)
    csv_reader = pa_csv.open_csv(
        reader,
        read_options=pa_csv.ReadOptions(block_size=64 << 20),
        parse_options=parse_opts,
        convert_options=convert_opts,
    )
    schema = csv_reader.schema
    file_seq = FILENAME_REGEX.search(zstd_path.name)
    file_id = file_seq.group(1) if file_seq else zstd_path.stem
    schema_with_extras = schema.append(pa.field("year", pa.string())).append(
        pa.field("file", pa.string())
    )

    def _batch_generator() -> Generator[pa.RecordBatch, None, None]:
        batch_count = 0
        try:
            for batch in csv_reader:
                year_array = pa.array([year] * batch.num_rows, type=pa.string())
                file_array = pa.array([file_id] * batch.num_rows, type=pa.string())
                batch_with_extras = pa.RecordBatch.from_arrays(
                    batch.columns + [year_array, file_array],
                    names=schema_with_extras.names,
                )
                batch_count += 1
                yield batch_with_extras
        except Exception as e:
            logger.exception("Error fetching batch for %s: %s", zstd_path, str(e))
            raise
        finally:
            csv_reader.close()
            reader.close()
            zstd_file.close()

    return _batch_generator(), schema_with_extras