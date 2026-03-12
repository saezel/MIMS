# Metadata Indexing & Management System (MIMS)

MIMS is a Python desktop application for importing metadata from HTML iframe payloads into SQLite and managing the indexed records through a GUI.

## Clean release behavior

This package ships as a clean release:

- no bundled `metadata_index.db`
- no preloaded rows
- no saved HTML folder path
- no saved database path
- no import error log

On first launch, the table is empty until you open an existing database or create a new one.

## Requirements

- Python 3.10+
- `lzstring`
- Tkinter support in your Python installation

Install the dependency:

```bash
pip install -r requirements.txt
```

## Launch

```bash
python main.py
```

Optional preload of an existing database:

```bash
python main.py /path/to/metadata_index.db
```

## Main workflow

1. Click **Open Existing DB** to select and open an existing SQLite database, or **Create New DB** to create and open a new one.
2. Choose an HTML folder if you want to import files.
3. Click **Import HTML Folder**.
4. Browse, search, filter, and manage rows from the main table.

## Import behavior

- Imports HTML files from a selected folder
- Decodes supported iframe payloads and writes rows into SQLite
- Continues past malformed files or rows where possible
- Shows live progress in the status bar:
  - files processed / total files
  - rows imported
  - errors
- Writes an import error log beside the database when needed

## Table features

- Sort by clicking column headers
- Drag headers to reorder visible columns
- Right-click the header or blank table area to show or hide columns
- Pagination for large datasets
- `Ctrl+A` selects all currently shown rows in the table
- Filesizes are displayed in human-readable decimal units:
  - B, KB, MB, GB, TB, PB
  - rounded to up to 3 decimal places
  - examples: `321.796 GB`, `9.877 GB`, `450 MB`

## Search

- Search runs when you press **Enter** in the search box or click **Search**
- Search scope:
  - All fields
  - Category
  - Source Title
  - Title
- Match mode:
  - `any` = match any search word
  - `all` = match all search words

## Advanced filters

Use **Advanced Filters...** to build multiple filter rules in a popup.

- Add or remove rules
- Reuse the same column more than once
- All rules are combined with **AND** logic
- Good for ranges such as:
  - `Filesize > 0 MB`
  - `Filesize < 500 GB`

Filesize rules now use a unit dropdown, ordered from largest to smallest:

- `(PB) Petabytes`
- `(TB) Terabytes`
- `(GB) Gigabytes`
- `(MB) Megabytes`
- `(KB) Kilobytes`
- `( B) Bytes`

Supported operators:

- Text columns:
  - contains
  - equals
  - starts with
  - ends with
  - is empty
  - is not empty
- Integer columns such as filesize:
  - `=`
  - `!=`
  - `<`
  - `<=`
  - `>`
  - `>=`
  - is empty
  - is not empty

For filesize filters, the entered number is interpreted using the selected unit and then matched against the raw byte value stored in SQLite.

## Categories

- Create categories
- Delete category definitions
- Assign categories to one or many selected rows
- Clear category assignments by assigning `(empty)`

## Row actions

Toolbar actions:

- Add row
- Modify selected row
- Delete selected row(s)
- Assign category to selected row(s)
- Manage categories
- Copy selected link(s)
- Open advanced filters
- Clear filters

Right-click row menu:

- Copy Selected Link(s)
- Copy Selected Source Title(s)
- Copy Selected Title(s)
- Delete Selected Row(s)

When copying multiple values, they are joined with a comma and a space.

## Notes

- `info_hash` is unique in the database
- the resource link is generated from the info hash as a magnet URI
- large databases are supported through indexed SQLite queries and paging
- edit dialogs still use raw filesize bytes for accuracy; the table view shows formatted sizes

## Project structure

- `main.py` — main GUI entry point
- `mims/gui.py` — Tkinter application
- `mims/db.py` — SQLite schema and query layer
- `mims/scraper.py` — HTML decoding and bulk import logic

## Running from source

```bash
python main.py
```
