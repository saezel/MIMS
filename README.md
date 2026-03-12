# Metadata Indexing & Management System (MIMS)

MIMS is a Python desktop application for importing metadata from HTML iframe payloads into SQLite and managing the indexed records through a GUI.

## Clean release behavior

This package is intentionally shipped as a clean release:

- no bundled `metadata_index.db`
- no preloaded rows
- no saved HTML folder path
- no saved database path
- no import error log

On first launch, the table is empty until you use **Open Existing DB** or **Create New DB**.

## Requirements

- Python 3.10+
- `lzstring`
- Tkinter support in your Python installation

Install the only external dependency:

```bash
pip install -r requirements.txt
```

## Launch

Start the full application with:

```bash
python main.py
```

Optional preload of an existing database:

```bash
python main.py /path/to/metadata_index.db
```

## Main GUI workflow

1. Click **Open Existing DB** to select and open an existing SQLite database, or **Create New DB** to create and open a new one.
3. Choose an HTML folder if you want to import files.
4. Run the import.
5. Browse and manage rows from the main table.

## Import behavior

- Imports HTML files from a selected folder
- Decodes supported iframe payloads using `lzstring`
- Preserves `source_title` when the decoded payload includes a wrapper title
- Leaves `category` empty on import
- Prevents duplicate `info_hash` values
- Updates existing rows on re-import when the same `info_hash` appears again
- Continues importing when malformed files or bad rows are encountered
- Strips invalid Unicode surrogate characters before writing text to SQLite
- Writes skipped-file and skipped-row errors to a TSV log beside the database

## Database schema

### `metadata_index`
- `id`
- `title`
- `category`
- `source_title`
- `filesize_bytes`
- `info_hash`
- `resource_link`

### `categories`
- `name`

## GUI features

- Open an existing SQLite database immediately with **Open Existing DB**
- Create a new SQLite database immediately with **Create New DB**
- Import HTML directly from a selected folder
- Browse large datasets with pagination
- Sort by clicking column headers
- Right-click the table header area to show or hide columns
- Drag column headers to reorder visible columns
- Add, edit, and delete records
- Create categories
- Assign categories to one or many selected rows
- Clear categories by assigning `(empty)`
- Copy the selected `resource_link` to the clipboard

## Search

Search runs only when you press **Enter** in the search box or click **Search**.

### Search scope options
- All fields
- Category
- Source Title
- Title

### Match modes
- `any` — returns rows matching any search word
- `all` — returns rows matching every search word

## Filtering

The filter controls support both text and numeric filtering.

### Text operators
- contains
- equals
- starts with
- ends with
- is empty
- is not empty

### Numeric operators
- `=`
- `!=`
- `<`
- `<=`
- `>`
- `>=`
- is empty
- is not empty

## Import progress

During import, the status bar shows:

- HTML files processed
- total HTML files detected
- rows imported
- error count

## Resource link format

`resource_link` is generated as:

```text
magnet:?xt=urn:btih:<info_hash>
```

If you need a different format, update `build_resource_link()` in `mims/db.py`.

## Optional CLI import

You can also run the importer directly:

```bash
python index_metadata.py /path/to/html_dir /path/to/metadata_index.db --verbose --error-log import_errors.tsv
```

## Project layout

```text
mims_project_v10_clean_release/
├── main.py
├── index_metadata.py
├── launch_gui.py
├── requirements.txt
├── README.md
└── mims/
    ├── __init__.py
    ├── db.py
    ├── gui.py
    └── scraper.py
```

## Notes

- `info_hash` is unique and cannot have duplicates
- this clean package does not include sample data
- a database file will only be created when you explicitly use **Create New DB**
