import zstandard as zstd
from pathlib import Path

def get_first_row_size_dynamic(zstd_path: Path) -> int:
    with zstd_path.open("rb") as zstd_file, zstd.ZstdDecompressor().stream_reader(zstd_file) as reader:
        row_data = b""
        while True:
            chunk = reader.read(512)  # Read small chunks
            if not chunk:
                break  # End of file reached before finding newline
            row_data += chunk
            newline_index = row_data.find(b"\n")
            if newline_index != -1:
                return newline_index + 1

        raise ValueError("No newline found in the decompressed stream. Is this a valid CSV?")


directory = Path("2024_awards")
for zstd_file in sorted(directory.glob("*.csv.zst")):
    try:
        first_row_size = get_first_row_size_dynamic(zstd_file)
        print(f"{zstd_file.name}: First row size = {first_row_size} bytes")
    except Exception as e:
        print(f"Error processing {zstd_file.name}: {e}")
