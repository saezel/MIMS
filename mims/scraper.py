from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Iterator, Sequence
from urllib.parse import urlsplit

from .db import build_resource_link, bulk_upsert_records, connect, init_db

LOGGER = logging.getLogger("mims.scraper")


def strip_invalid_unicode(value: object | None) -> str:
    if value is None:
        return ""
    text = str(value)
    return text.encode("utf-8", "ignore").decode("utf-8", "ignore")


def safe_log_write(handle, message: str) -> None:
    handle.write(message.encode("utf-8", "backslashreplace").decode("utf-8"))


def upsert_records_resilient(
    conn,
    records: Sequence[Sequence[object]],
    *,
    batch_size: int,
    error_handle,
    source_file: Path,
) -> tuple[int, int]:
    if not records:
        return (0, 0)

    try:
        return (bulk_upsert_records(conn, records, batch_size=batch_size), 0)
    except Exception as exc:
        LOGGER.warning("Bulk import fallback for %s: %s", source_file, exc)
        row_count = 0
        error_count = 0
        for row in records:
            try:
                row_count += bulk_upsert_records(conn, [row], batch_size=1)
            except Exception as row_exc:
                error_count += 1
                if error_handle:
                    safe_log_write(
                        error_handle,
                        f"{source_file}	row	{row[4] if len(row) > 4 else ''}	{row_exc}\n",
                    )
        return (row_count, error_count)

_URI_SAFE_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-$"
_URI_SAFE_LOOKUP = {ch: idx for idx, ch in enumerate(_URI_SAFE_ALPHABET)}


class LZStringCompat:
    def decompressFromEncodedURIComponent(self, data: str | None) -> str | None:
        if data is None:
            return None
        if data == "":
            return ""
        data = data.replace(" ", "+")
        return self._decompress(len(data), 32, lambda index: _URI_SAFE_LOOKUP[data[index]])

    def _decompress(self, length: int, reset_value: int, get_next_value: Callable[[int], int]) -> str:
        dictionary: dict[int, str] = {0: "", 1: "", 2: ""}
        enlarge_in = 4
        dict_size = 4
        num_bits = 3
        entry = ""
        result: list[str] = []

        data_val = get_next_value(0)
        data_position = reset_value
        data_index = 1

        def read_bits(nbits: int) -> int:
            nonlocal data_val, data_position, data_index
            bits = 0
            maxpower = 1 << nbits
            power = 1
            while power != maxpower:
                resb = data_val & data_position
                data_position >>= 1
                if data_position == 0:
                    data_position = reset_value
                    if data_index < length:
                        data_val = get_next_value(data_index)
                    else:
                        data_val = 0
                    data_index += 1
                if resb > 0:
                    bits |= power
                power <<= 1
            return bits

        next_value = read_bits(2)
        if next_value == 0:
            c = chr(read_bits(8))
        elif next_value == 1:
            c = chr(read_bits(16))
        elif next_value == 2:
            return ""
        else:
            raise ValueError("invalid LZString stream header")

        dictionary[3] = c
        w = c
        result.append(c)

        while True:
            if data_index > length + 1:
                raise ValueError("corrupt LZString stream")

            c_num = read_bits(num_bits)

            if c_num == 0:
                dictionary[dict_size] = chr(read_bits(8))
                c_num = dict_size
                dict_size += 1
                enlarge_in -= 1
            elif c_num == 1:
                dictionary[dict_size] = chr(read_bits(16))
                c_num = dict_size
                dict_size += 1
                enlarge_in -= 1
            elif c_num == 2:
                return "".join(result)

            if enlarge_in == 0:
                enlarge_in = 1 << num_bits
                num_bits += 1

            if c_num in dictionary:
                entry = dictionary[c_num]
            elif c_num == dict_size:
                entry = w + w[0]
            else:
                raise ValueError("invalid dictionary index while decompressing")

            result.append(entry)
            dictionary[dict_size] = w + entry[0]
            dict_size += 1
            enlarge_in -= 1
            w = entry

            if enlarge_in == 0:
                enlarge_in = 1 << num_bits
                num_bits += 1


@dataclass(slots=True)
class MetadataRecord:
    title: str
    category: str
    source_title: str | None
    filesize_bytes: int
    info_hash: str
    resource_link: str

    def as_sql_tuple(self) -> tuple[str, str, str | None, int, str, str]:
        return (
            self.title,
            self.category,
            self.source_title,
            self.filesize_bytes,
            self.info_hash,
            self.resource_link,
        )


class IframeSrcParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.sources: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "iframe":
            return
        attr_map = dict(attrs)
        src = attr_map.get("src")
        if src:
            self.sources.append(src)


class MetadataDecodeError(Exception):
    pass


class MetadataScraper:
    def __init__(self) -> None:
        self._lz = LZStringCompat()

    def extract_iframe_sources(self, html_text: str) -> list[str]:
        parser = IframeSrcParser()
        parser.feed(html_text)
        return parser.sources

    def extract_payload(self, src: str) -> str:
        parsed = urlsplit(src)
        if parsed.fragment:
            return parsed.fragment
        if "#" in src:
            return src.split("#", 1)[1]
        raise MetadataDecodeError("iframe src did not contain a fragment payload")

    def decode_payload(self, compressed_payload: str):
        try:
            decompressed = self._lz.decompressFromEncodedURIComponent(compressed_payload)
        except Exception as exc:
            raise MetadataDecodeError(f"lz-string decompression failed: {exc}") from exc
        if decompressed is None:
            raise MetadataDecodeError("lz-string decompression returned null")
        if decompressed == "":
            raise MetadataDecodeError("lz-string decompression returned empty content")
        try:
            return json.loads(decompressed)
        except json.JSONDecodeError as exc:
            raise MetadataDecodeError(f"invalid JSON after decompression: {exc}") from exc

    def extract_records_from_object(self, obj) -> list[MetadataRecord]:
        source_title, candidates = self._extract_container_context(obj)
        records: list[MetadataRecord] = []
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            if {"filename", "bytes", "hash"}.issubset(candidate.keys()):
                try:
                    title = strip_invalid_unicode(candidate["filename"]).strip()
                    filesize_bytes = int(candidate["bytes"])
                    info_hash = strip_invalid_unicode(candidate["hash"]).strip()
                except (TypeError, ValueError):
                    continue
                if not title or not info_hash:
                    continue
                records.append(
                    MetadataRecord(
                        title=title,
                        category="",
                        source_title=source_title,
                        filesize_bytes=filesize_bytes,
                        info_hash=info_hash,
                        resource_link=build_resource_link(info_hash),
                    )
                )
        if not records:
            raise MetadataDecodeError("no metadata objects containing filename/bytes/hash were found")
        return records

    def _extract_container_context(self, obj) -> tuple[str | None, Iterator[object]]:
        if isinstance(obj, dict) and isinstance(obj.get("torrents"), list):
            source_title = strip_invalid_unicode(obj.get("title", "")).strip() or None
            return (source_title, iter(obj["torrents"]))
        if isinstance(obj, list):
            return (None, iter(obj))

        source_title: str | None = None
        if isinstance(obj, dict):
            raw_title = obj.get("title")
            if raw_title is not None:
                source_title = strip_invalid_unicode(raw_title).strip() or None
        return (source_title, self._walk_objects(obj))

    def parse_html_file(self, file_path: Path) -> list[MetadataRecord]:
        try:
            html_text = file_path.read_text(encoding="utf-8", errors="ignore")
        except OSError as exc:
            raise MetadataDecodeError(f"unable to read file: {exc}") from exc

        sources = self.extract_iframe_sources(html_text)
        if not sources:
            return []

        all_records: list[MetadataRecord] = []
        for src in sources:
            payload = self.extract_payload(src)
            decoded = self.decode_payload(payload)
            all_records.extend(self.extract_records_from_object(decoded))
        return all_records

    def _walk_objects(self, obj) -> Iterator[object]:
        yield obj
        if isinstance(obj, dict):
            for value in obj.values():
                yield from self._walk_objects(value)
        elif isinstance(obj, list):
            for item in obj:
                yield from self._walk_objects(item)


def iter_html_files(input_dir: Path) -> Iterator[Path]:
    for path in sorted(input_dir.rglob("*")):
        if path.is_file() and path.suffix.lower() in {".html", ".htm"}:
            yield path


def index_directory(
    input_dir: Path,
    db_path: Path,
    *,
    batch_size: int = 5000,
    log_every: int = 250,
    error_log: Path | None = None,
    progress_callback: Callable[[dict[str, int]], None] | None = None,
) -> dict[str, int]:
    scraper = MetadataScraper()
    conn = connect(db_path)
    init_db(conn)

    html_files = list(iter_html_files(input_dir))
    total_files = len(html_files)
    file_count = 0
    record_count = 0
    error_count = 0

    if progress_callback:
        progress_callback({
            "files_processed": 0,
            "rows_imported": 0,
            "errors": 0,
            "total_files": total_files,
        })

    error_handle = error_log.open("a", encoding="utf-8", errors="backslashreplace") if error_log else None
    try:
        for file_count, html_file in enumerate(html_files, start=1):
            try:
                records = scraper.parse_html_file(html_file)
                imported, row_errors = upsert_records_resilient(
                    conn,
                    [record.as_sql_tuple() for record in records],
                    batch_size=batch_size,
                    error_handle=error_handle,
                    source_file=html_file,
                )
                record_count += imported
                error_count += row_errors
            except MetadataDecodeError as exc:
                error_count += 1
                if error_handle:
                    safe_log_write(error_handle, f"{html_file}\tfile\t\t{exc}\n")
                LOGGER.warning("Skipping %s: %s", html_file, exc)
            except Exception as exc:
                error_count += 1
                if error_handle:
                    safe_log_write(error_handle, f"{html_file}\tfile\t\t{exc}\n")
                LOGGER.warning("Skipping %s due to unexpected error: %s", html_file, exc)

            if progress_callback:
                progress_callback({
                    "files_processed": file_count,
                    "rows_imported": record_count,
                    "errors": error_count,
                    "total_files": total_files,
                })

            if file_count % log_every == 0:
                LOGGER.info(
                    "Processed %s files | imported rows so far: %s | errors: %s",
                    file_count,
                    record_count,
                    error_count,
                )
    finally:
        if error_handle:
            error_handle.close()
        conn.close()

    summary = {
        "files_processed": file_count,
        "rows_imported": record_count,
        "errors": error_count,
        "total_files": total_files,
    }
    if progress_callback:
        progress_callback(summary)
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Decode iframe metadata and import it into SQLite.")
    parser.add_argument("input_dir", type=Path, help="Directory containing HTML files")
    parser.add_argument("db_path", type=Path, help="SQLite database output path")
    parser.add_argument("--batch-size", type=int, default=5000, help="SQLite bulk upsert batch size")
    parser.add_argument("--log-every", type=int, default=250, help="Progress log interval in files")
    parser.add_argument("--error-log", type=Path, default=None, help="Optional tab-separated error log file")
    parser.add_argument("--verbose", action="store_true", help="Enable info-level logging")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    summary = index_directory(
        input_dir=args.input_dir,
        db_path=args.db_path,
        batch_size=args.batch_size,
        log_every=args.log_every,
        error_log=args.error_log,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
