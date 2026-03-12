from __future__ import annotations

import argparse

from mims.gui import launch_gui


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch the Metadata Indexing & Management System GUI.")
    parser.add_argument(
        "db_path",
        nargs="?",
        default=None,
        help="Optional SQLite database path to preload.",
    )
    args = parser.parse_args()
    launch_gui(args.db_path)


if __name__ == "__main__":
    main()
