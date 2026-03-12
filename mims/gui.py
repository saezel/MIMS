from __future__ import annotations

import math
import queue
import sqlite3
import threading
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Literal

from .db import (
    add_category_definition,
    add_record,
    assign_category,
    connect,
    count_records,
    delete_category_definition,
    delete_record,
    fetch_records,
    get_record_by_id,
    init_db,
    list_categories,
    update_record,
)
from .scraper import index_directory

ALL_COLUMNS = ("id", "category", "source_title", "title", "filesize_bytes", "info_hash", "resource_link")
COLUMN_HEADINGS = {
    "id": "ID",
    "category": "Category",
    "source_title": "Source Title",
    "title": "Title",
    "filesize_bytes": "Filesize",
    "info_hash": "Info Hash",
    "resource_link": "Resource Link",
}
COLUMN_WIDTHS = {
    "id": 70,
    "category": 130,
    "source_title": 220,
    "title": 360,
    "filesize_bytes": 120,
    "info_hash": 250,
    "resource_link": 420,
}
FILTER_COLUMNS = {
    "ID": "id",
    "Category": "category",
    "Source Title": "source_title",
    "Title": "title",
    "Filesize": "filesize_bytes",
    "Info Hash": "info_hash",
    "Resource Link": "resource_link",
}
TEXT_FILTER_OPERATORS = ("contains", "equals", "starts with", "ends with", "is empty", "is not empty")
NUMBER_FILTER_OPERATORS = ("=", "!=", "<", "<=", ">", ">=", "is empty", "is not empty")
INTEGER_FILTER_COLUMNS = {"id", "filesize_bytes"}
SEARCH_SCOPE_LABEL_TO_VALUE = {
    "All fields": "all",
    "Category": "category",
    "Source Title": "source_title",
    "Title": "title",
}
SEARCH_SCOPE_VALUE_TO_LABEL = {value: label for label, value in SEARCH_SCOPE_LABEL_TO_VALUE.items()}
FILESIZE_UNIT_LABELS = (
    "(PB) Petabytes",
    "(TB) Terabytes",
    "(GB) Gigabytes",
    "(MB) Megabytes",
    "(KB) Kilobytes",
    "( B) Bytes",
)
FILESIZE_UNIT_LABEL_TO_SYMBOL = {
    "(PB) Petabytes": "PB",
    "(TB) Terabytes": "TB",
    "(GB) Gigabytes": "GB",
    "(MB) Megabytes": "MB",
    "(KB) Kilobytes": "KB",
    "( B) Bytes": "B",
}
FILESIZE_UNIT_SYMBOL_TO_LABEL = {value: key for key, value in FILESIZE_UNIT_LABEL_TO_SYMBOL.items()}

SortColumn = Literal["id", "category", "source_title", "title", "filesize_bytes", "info_hash", "resource_link"]


def format_filesize(value: object) -> str:
    try:
        size = int(value)
    except (TypeError, ValueError):
        return str(value)
    if size == 0:
        return "0 B"
    units = ("B", "KB", "MB", "GB", "TB", "PB")
    display = float(size)
    unit_index = 0
    while display >= 1000 and unit_index < len(units) - 1:
        display /= 1000.0
        unit_index += 1
    number = f"{display:.3f}".rstrip("0").rstrip(".")
    return f"{number} {units[unit_index]}"


def filter_rule_summary(rule: dict[str, str]) -> str:
    column = rule.get("column", "")
    operator = rule.get("operator", "")
    value = (rule.get("value", "") or "").strip()
    label = COLUMN_HEADINGS.get(column, column or "Field")
    if operator in {"is empty", "is not empty"}:
        return f"{label} {operator}"
    if column == "filesize_bytes" and value:
        unit = (rule.get("unit", "B") or "B").strip().upper()
        return f"{label} {operator} {value} {unit}".strip()
    return f"{label} {operator} {value}".strip()


@dataclass(slots=True)
class AppState:
    page: int = 1
    page_size: int = 200
    search: str = ""
    search_mode: str = "any"
    search_field: str = "all"
    sort_column: SortColumn = "title"
    sort_desc: bool = False
    total_rows: int = 0
    filter_rules: list[dict[str, str]] | None = None


class CategoryManagerDialog(tk.Toplevel):
    def __init__(self, master: tk.Misc, conn: sqlite3.Connection) -> None:
        super().__init__(master)
        self.conn = conn
        self.title("Manage Categories")
        self.resizable(False, False)
        self.transient(master)
        self.grab_set()

        self.new_category_var = tk.StringVar()

        frame = ttk.Frame(self, padding=12)
        frame.grid(sticky="nsew")
        frame.columnconfigure(0, weight=1)

        ttk.Label(frame, text="Existing categories").grid(row=0, column=0, sticky="w")
        self.listbox = tk.Listbox(frame, height=12, width=36, exportselection=False)
        self.listbox.grid(row=1, column=0, sticky="nsew", pady=(6, 10))

        entry_row = ttk.Frame(frame)
        entry_row.grid(row=2, column=0, sticky="ew")
        entry_row.columnconfigure(0, weight=1)

        ttk.Entry(entry_row, textvariable=self.new_category_var).grid(row=0, column=0, sticky="ew")
        ttk.Button(entry_row, text="Create", command=self.create_category).grid(row=0, column=1, padx=(8, 0))

        actions = ttk.Frame(frame)
        actions.grid(row=3, column=0, sticky="e", pady=(10, 0))
        ttk.Button(actions, text="Delete Selected", command=self.delete_selected).grid(row=0, column=0, padx=4)
        ttk.Button(actions, text="Close", command=self.destroy).grid(row=0, column=1, padx=4)

        self.bind("<Return>", lambda _e: self.create_category())
        self.refresh_categories()
        self.wait_visibility()
        self.focus_force()

    def refresh_categories(self) -> None:
        categories = list_categories(self.conn)
        self.listbox.delete(0, tk.END)
        for category in categories:
            self.listbox.insert(tk.END, category)

    def create_category(self) -> None:
        name = self.new_category_var.get().strip()
        if not name:
            messagebox.showerror("Validation error", "Category name cannot be empty.", parent=self)
            return
        try:
            add_category_definition(self.conn, name)
        except ValueError as exc:
            messagebox.showerror("Category error", str(exc), parent=self)
            return
        self.new_category_var.set("")
        self.refresh_categories()

    def delete_selected(self) -> None:
        selection = self.listbox.curselection()
        if not selection:
            messagebox.showinfo("No selection", "Select a category first.", parent=self)
            return
        name = str(self.listbox.get(selection[0]))
        if not messagebox.askyesno(
            "Delete category",
            f"Delete '{name}' and clear it from any assigned rows?",
            parent=self,
        ):
            return
        delete_category_definition(self.conn, name)
        self.refresh_categories()


class AssignCategoryDialog(tk.Toplevel):
    def __init__(self, master: tk.Misc, categories: list[str], current: str = "") -> None:
        super().__init__(master)
        self.title("Assign Category")
        self.resizable(False, False)
        self.transient(master)
        self.grab_set()
        self.result: str | None = None

        initial_display = current if current else "(empty)"
        self.category_var = tk.StringVar(value=initial_display)
        values = ["(empty)", *categories]

        frame = ttk.Frame(self, padding=12)
        frame.grid(sticky="nsew")
        frame.columnconfigure(1, weight=1)

        ttk.Label(frame, text="Category").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        combo = ttk.Combobox(frame, textvariable=self.category_var, values=values, state="readonly", width=28)
        combo.grid(row=0, column=1, sticky="ew", pady=4)

        buttons = ttk.Frame(frame)
        buttons.grid(row=1, column=0, columnspan=2, sticky="e", pady=(10, 0))
        ttk.Button(buttons, text="Apply", command=self._save).grid(row=0, column=0, padx=4)
        ttk.Button(buttons, text="Cancel", command=self.destroy).grid(row=0, column=1, padx=4)

        self.bind("<Return>", lambda _e: self._save())
        self.bind("<Escape>", lambda _e: self.destroy())
        self.wait_visibility()
        self.focus_force()
        combo.focus_set()

    def _save(self) -> None:
        value = self.category_var.get().strip()
        self.result = "" if value in {"", "(empty)"} else value
        self.destroy()


class RecordDialog(tk.Toplevel):
    def __init__(self, master: tk.Misc, title_text: str, categories: list[str], initial: dict[str, str] | None = None) -> None:
        super().__init__(master)
        self.title(title_text)
        self.resizable(False, False)
        self.transient(master)
        self.grab_set()
        self.result: dict[str, str] | None = None

        initial_category = (initial or {}).get("category", "") or "(empty)"
        self.vars = {
            "title": tk.StringVar(value=(initial or {}).get("title", "")),
            "category": tk.StringVar(value=initial_category),
            "source_title": tk.StringVar(value=(initial or {}).get("source_title", "")),
            "filesize_bytes": tk.StringVar(value=(initial or {}).get("filesize_bytes", "")),
            "info_hash": tk.StringVar(value=(initial or {}).get("info_hash", "")),
        }

        frame = ttk.Frame(self, padding=12)
        frame.grid(sticky="nsew")
        frame.columnconfigure(1, weight=1)

        ttk.Label(frame, text="Title").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(frame, textvariable=self.vars["title"], width=60).grid(row=0, column=1, sticky="ew", pady=4)

        ttk.Label(frame, text="Category").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        category_combo = ttk.Combobox(
            frame,
            width=24,
            textvariable=self.vars["category"],
            values=["(empty)", *categories],
            state="readonly",
        )
        category_combo.grid(row=1, column=1, sticky="w", pady=4)

        ttk.Label(frame, text="Source title").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(frame, textvariable=self.vars["source_title"], width=60).grid(row=2, column=1, sticky="ew", pady=4)

        ttk.Label(frame, text="Filesize (bytes)").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(frame, textvariable=self.vars["filesize_bytes"], width=20).grid(row=3, column=1, sticky="ew", pady=4)

        ttk.Label(frame, text="Info hash").grid(row=4, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(frame, textvariable=self.vars["info_hash"], width=60).grid(row=4, column=1, sticky="ew", pady=4)

        buttons = ttk.Frame(frame)
        buttons.grid(row=5, column=0, columnspan=2, sticky="e", pady=(10, 0))
        ttk.Button(buttons, text="Save", command=self._save).grid(row=0, column=0, padx=4)
        ttk.Button(buttons, text="Cancel", command=self.destroy).grid(row=0, column=1, padx=4)

        self.bind("<Return>", lambda _e: self._save())
        self.bind("<Escape>", lambda _e: self.destroy())
        self.wait_visibility()
        self.focus_force()
        category_combo.focus_set()

    def _save(self) -> None:
        title = self.vars["title"].get().strip()
        raw_category = self.vars["category"].get().strip()
        category = "" if raw_category in {"", "(empty)"} else raw_category
        source_title = self.vars["source_title"].get().strip()
        filesize = self.vars["filesize_bytes"].get().strip()
        info_hash = self.vars["info_hash"].get().strip()

        if not title:
            messagebox.showerror("Validation error", "Title is required.", parent=self)
            return
        if not info_hash:
            messagebox.showerror("Validation error", "Info hash is required.", parent=self)
            return
        try:
            filesize_int = int(filesize)
            if filesize_int < 0:
                raise ValueError
        except ValueError:
            messagebox.showerror("Validation error", "Filesize must be a non-negative integer.", parent=self)
            return

        self.result = {
            "title": title,
            "category": category,
            "source_title": source_title,
            "filesize_bytes": str(filesize_int),
            "info_hash": info_hash,
        }
        self.destroy()


MAX_FILTER_RULES = 20


class AdvancedFiltersDialog(tk.Toplevel):
    def __init__(self, master: tk.Misc, initial_rules: list[dict[str, str]] | None = None) -> None:
        super().__init__(master)
        self.title("Advanced Filters")
        self.resizable(True, False)
        self.transient(master)
        self.grab_set()
        self.result: list[dict[str, str]] | None = None
        self.rule_rows: list[dict[str, object]] = []

        frame = ttk.Frame(self, padding=12)
        frame.grid(sticky="nsew")
        frame.columnconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)

        ttk.Label(
            frame,
            text="Add one or more rules. All rules are applied together with AND logic.",
        ).grid(row=0, column=0, sticky="w")

        self.rules_container = ttk.Frame(frame)
        self.rules_container.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        self.rules_container.columnconfigure(2, weight=1)

        button_row = ttk.Frame(frame)
        button_row.grid(row=2, column=0, sticky="ew", pady=(12, 0))
        button_row.columnconfigure(1, weight=1)

        ttk.Button(button_row, text="Add Rule", command=self.add_rule).grid(row=0, column=0, sticky="w")

        actions = ttk.Frame(button_row)
        actions.grid(row=0, column=2, sticky="e")
        ttk.Button(actions, text="Apply", command=self._apply).grid(row=0, column=0, padx=4)
        ttk.Button(actions, text="Cancel", command=self.destroy).grid(row=0, column=1, padx=4)

        rules_to_load = list(initial_rules or [])[:MAX_FILTER_RULES]
        for rule in rules_to_load:
            self.add_rule(rule)

        self.bind("<Return>", lambda _e: self._apply())
        self.bind("<Escape>", lambda _e: self.destroy())
        self.wait_visibility()
        self.focus_force()

    def add_rule(self, initial: dict[str, str] | None = None) -> None:
        if len(self.rule_rows) >= MAX_FILTER_RULES:
            messagebox.showinfo("Rule limit reached", f"You can add up to {MAX_FILTER_RULES} filter rules.", parent=self)
            return
        initial = initial or {}
        selected_column = initial.get("column", "filesize_bytes")
        display_label = next((label for label, value in FILTER_COLUMNS.items() if value == selected_column), "Filesize")
        initial_unit = (initial.get("unit", "B") or "B").strip().upper()
        if initial_unit not in FILESIZE_UNIT_SYMBOL_TO_LABEL:
            initial_unit = "B"

        row: dict[str, object] = {
            "column_var": tk.StringVar(value=display_label),
            "operator_var": tk.StringVar(value=initial.get("operator", ">")),
            "value_var": tk.StringVar(value=initial.get("value", "")),
            "unit_var": tk.StringVar(value=FILESIZE_UNIT_SYMBOL_TO_LABEL[initial_unit]),
        }

        row["label"] = ttk.Label(self.rules_container, text="")
        row["field_combo"] = ttk.Combobox(
            self.rules_container,
            width=16,
            textvariable=row["column_var"],
            values=list(FILTER_COLUMNS.keys()),
            state="readonly",
        )
        row["operator_combo"] = ttk.Combobox(
            self.rules_container,
            width=14,
            textvariable=row["operator_var"],
            state="readonly",
        )
        row["value_entry"] = ttk.Entry(self.rules_container, textvariable=row["value_var"])
        row["unit_combo"] = ttk.Combobox(
            self.rules_container,
            width=18,
            textvariable=row["unit_var"],
            values=FILESIZE_UNIT_LABELS,
            state="readonly",
        )
        row["remove_button"] = ttk.Button(
            self.rules_container,
            text="Remove",
            command=lambda r=row: self.remove_rule(r),
        )

        def on_column_change(_event=None, rule_row=row) -> None:
            self._refresh_rule_operator_choices(rule_row)

        row["field_combo"].bind("<<ComboboxSelected>>", on_column_change)

        self.rule_rows.append(row)
        self._layout_rule_rows()
        self._refresh_rule_operator_choices(row)

    def remove_rule(self, row: dict[str, object]) -> None:
        if row not in self.rule_rows:
            return
        for widget_key in ("label", "field_combo", "operator_combo", "value_entry", "unit_combo", "remove_button"):
            widget = row.get(widget_key)
            if widget is not None:
                widget.destroy()
        self.rule_rows.remove(row)
        self._layout_rule_rows()

    def _layout_rule_rows(self) -> None:
        for index, row in enumerate(self.rule_rows):
            row["label"].configure(text=f"Rule {index + 1}")
            row["label"].grid(row=index, column=0, sticky="w", padx=(0, 8), pady=4)
            row["field_combo"].grid(row=index, column=1, sticky="w", padx=(0, 8), pady=4)
            row["operator_combo"].grid(row=index, column=2, sticky="w", padx=(0, 8), pady=4)
            row["value_entry"].grid(row=index, column=3, sticky="ew", padx=(0, 8), pady=4)
            row["unit_combo"].grid(row=index, column=4, sticky="w", padx=(0, 8), pady=4)
            row["remove_button"].grid(row=index, column=5, sticky="w", pady=4)
        self.rules_container.columnconfigure(3, weight=1)

    def _refresh_rule_operator_choices(self, row: dict[str, object]) -> None:
        selected_label = str(row["column_var"].get()).strip() or "Filesize"
        column = FILTER_COLUMNS.get(selected_label, "filesize_bytes")
        values = NUMBER_FILTER_OPERATORS if column in INTEGER_FILTER_COLUMNS else TEXT_FILTER_OPERATORS
        operator_combo = row["operator_combo"]
        operator_combo.configure(values=values)
        current_operator = str(row["operator_var"].get()).strip()
        if current_operator not in values:
            row["operator_var"].set(values[0])

        unit_combo = row["unit_combo"]
        if column == "filesize_bytes":
            unit_combo.grid()
        else:
            unit_combo.grid_remove()

    def _apply(self) -> None:
        rules: list[dict[str, str]] = []
        for row in self.rule_rows[:MAX_FILTER_RULES]:
            label = str(row["column_var"].get()).strip()
            column = FILTER_COLUMNS.get(label, "")
            operator = str(row["operator_var"].get()).strip()
            value = str(row["value_var"].get()).strip()
            unit_label = str(row["unit_var"].get()).strip() or "( B) Bytes"
            unit_symbol = FILESIZE_UNIT_LABEL_TO_SYMBOL.get(unit_label, "B")

            if not column or not operator:
                continue
            if operator not in {"is empty", "is not empty"} and value == "":
                continue
            if column in INTEGER_FILTER_COLUMNS and operator not in {"is empty", "is not empty"}:
                try:
                    int(value)
                except ValueError:
                    continue

            rule = {"column": column, "operator": operator, "value": value}
            if column == "filesize_bytes":
                rule["unit"] = unit_symbol
            rules.append(rule)

        self.result = rules
        self.destroy()


class MetadataManagerApp(ttk.Frame):
    def __init__(self, master: tk.Tk, db_path: str | None = None) -> None:
        super().__init__(master, padding=10)
        self.master = master
        self.conn: sqlite3.Connection | None = None
        self.state = AppState()
        self.search_var = tk.StringVar()
        self.search_mode_var = tk.StringVar(value="any")
        self.search_field_var = tk.StringVar(value="All fields")
        self.db_path_var = tk.StringVar(value=str(Path(db_path).expanduser()) if db_path else "")
        self.html_dir_var = tk.StringVar(value="")
        self.status_var = tk.StringVar(value="No database opened. Select or create a SQLite DB to begin.")
        self.import_thread: threading.Thread | None = None
        self.import_result: tuple[bool, str] | None = None
        self.import_progress_queue: queue.Queue[dict[str, int]] = queue.Queue()
        self.result_cache_max_rows = 5000
        self._result_cache_signature: tuple[object, ...] | None = None
        self._result_cache_rows: list[sqlite3.Row] | None = None

        self.filter_summary_var = tk.StringVar(value="Filters: none")

        self.column_order = list(ALL_COLUMNS)
        self.visible_columns = set(ALL_COLUMNS)
        self.column_menu: tk.Menu | None = None
        self.row_menu: tk.Menu | None = None
        self.column_visibility_vars: dict[str, tk.BooleanVar] = {}
        self.drag_source_column: str | None = None
        self.category_cache: list[str] = []

        self.grid(sticky="nsew")
        self._build_ui()
        self._rebuild_column_menu()
        self._refresh_filter_summary()
        if self.db_path_var.get().strip():
            self.open_database(self.db_path_var.get())
        else:
            self._reset_table_view()

    def _build_ui(self) -> None:
        self.master.title("Metadata Indexing & Management System")
        self.master.geometry("1540x860")
        self.master.minsize(1120, 660)
        self.master.columnconfigure(0, weight=1)
        self.master.rowconfigure(0, weight=1)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(4, weight=1)

        db_frame = ttk.LabelFrame(self, text="Database")
        db_frame.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        db_frame.columnconfigure(1, weight=1)

        ttk.Label(db_frame, text="SQLite DB:").grid(row=0, column=0, sticky="w", padx=(8, 6), pady=8)
        db_path_entry = ttk.Entry(db_frame, textvariable=self.db_path_var, state="readonly")
        db_path_entry.grid(row=0, column=1, sticky="ew", pady=8)
        ttk.Button(db_frame, text="Open Existing DB", command=self.open_existing_db).grid(row=0, column=2, padx=4, pady=8)
        ttk.Button(db_frame, text="Create New DB", command=self.create_new_db).grid(row=0, column=3, padx=(4, 8), pady=8)

        import_frame = ttk.LabelFrame(self, text="Import")
        import_frame.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        import_frame.columnconfigure(1, weight=1)

        ttk.Label(import_frame, text="HTML folder:").grid(row=0, column=0, sticky="w", padx=(8, 6), pady=8)
        ttk.Entry(import_frame, textvariable=self.html_dir_var).grid(row=0, column=1, sticky="ew", pady=8)
        ttk.Button(import_frame, text="Browse…", command=self.browse_html_dir).grid(row=0, column=2, padx=4, pady=8)
        self.import_button = ttk.Button(import_frame, text="Import HTML Folder", command=self.start_import)
        self.import_button.grid(row=0, column=3, padx=(4, 8), pady=8)

        search_frame = ttk.LabelFrame(self, text="Search")
        search_frame.grid(row=2, column=0, sticky="ew", pady=(0, 8))
        search_frame.columnconfigure(1, weight=1)

        ttk.Label(search_frame, text="Search text:").grid(row=0, column=0, sticky="w", padx=(8, 6), pady=(8, 6))
        search_entry = ttk.Entry(search_frame, textvariable=self.search_var)
        search_entry.grid(row=0, column=1, sticky="ew", pady=(8, 6))
        search_entry.bind("<Return>", self._on_search_submitted)

        ttk.Button(search_frame, text="Search", command=self.apply_search).grid(row=0, column=2, padx=4, pady=(8, 6))
        ttk.Button(search_frame, text="Clear", command=self.clear_search).grid(row=0, column=3, padx=(4, 8), pady=(8, 6))

        ttk.Label(search_frame, text="Search in:").grid(row=1, column=0, sticky="w", padx=(8, 6), pady=(0, 8))
        search_field_combo = ttk.Combobox(
            search_frame,
            width=18,
            textvariable=self.search_field_var,
            values=tuple(SEARCH_SCOPE_LABEL_TO_VALUE.keys()),
            state="readonly",
        )
        search_field_combo.grid(row=1, column=1, sticky="w", pady=(0, 8))
        search_field_combo.bind("<<ComboboxSelected>>", self._on_search_field_changed)

        ttk.Label(search_frame, text="Match:").grid(row=1, column=2, sticky="e", padx=(8, 4), pady=(0, 8))
        search_mode_combo = ttk.Combobox(
            search_frame,
            width=12,
            textvariable=self.search_mode_var,
            values=("any", "all"),
            state="readonly",
        )
        search_mode_combo.grid(row=1, column=3, sticky="w", padx=(0, 8), pady=(0, 8))
        search_mode_combo.bind("<<ComboboxSelected>>", self._on_search_mode_changed)

        ttk.Button(search_frame, text="Refresh", command=self.refresh_results).grid(row=1, column=4, padx=(0, 8), pady=(0, 8))

        toolbar = ttk.LabelFrame(self, text="Manage / Filter")
        toolbar.grid(row=3, column=0, sticky="ew", pady=(0, 8))
        toolbar.columnconfigure(7, weight=1)

        ttk.Button(toolbar, text="Add", command=self.add_row).grid(row=0, column=0, padx=(8, 4), pady=(8, 6))
        ttk.Button(toolbar, text="Modify", command=self.edit_selected).grid(row=0, column=1, padx=4, pady=(8, 6))
        ttk.Button(toolbar, text="Delete", command=self.delete_selected).grid(row=0, column=2, padx=4, pady=(8, 6))
        ttk.Button(toolbar, text="Assign Category", command=self.assign_category_to_selection).grid(row=0, column=3, padx=4, pady=(8, 6))
        ttk.Button(toolbar, text="Manage Categories", command=self.manage_categories).grid(row=0, column=4, padx=4, pady=(8, 6))
        ttk.Button(toolbar, text="Copy Link", command=self.copy_selected_link).grid(row=0, column=5, padx=4, pady=(8, 6))
        ttk.Button(toolbar, text="Advanced Filters...", command=self.open_advanced_filters).grid(row=0, column=6, padx=(4, 4), pady=(8, 6))
        ttk.Button(toolbar, text="Clear Filters", command=self.clear_filters).grid(row=0, column=7, padx=(4, 8), pady=(8, 6), sticky="w")

        ttk.Label(toolbar, textvariable=self.filter_summary_var).grid(
            row=1,
            column=0,
            columnspan=8,
            sticky="w",
            padx=(8, 8),
            pady=(0, 4),
        )
        ttk.Label(toolbar, text="Right-click rows for copy/delete. Right-click the header or blank table area to show/hide columns. Drag headers to reorder.").grid(
            row=2,
            column=0,
            columnspan=8,
            sticky="w",
            padx=(8, 8),
            pady=(0, 8),
        )

        table_frame = ttk.Frame(self)
        table_frame.grid(row=4, column=0, sticky="nsew")
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(table_frame, columns=ALL_COLUMNS, show="headings", selectmode="extended")
        self.tree.grid(row=0, column=0, sticky="nsew")

        for col in ALL_COLUMNS:
            self.tree.heading(col, text=COLUMN_HEADINGS[col], command=lambda c=col: self.toggle_sort(c))
            self.tree.column(col, width=COLUMN_WIDTHS[col], anchor="w", stretch=(col != "id"))

        yscroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        xscroll.grid(row=1, column=0, sticky="ew")
        self.tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)
        self.tree.configure(displaycolumns=self.column_order)

        self.tree.bind("<Button-3>", self._on_tree_right_click)
        self.tree.bind("<ButtonPress-1>", self._on_tree_button_press, add="+")
        self.tree.bind("<ButtonRelease-1>", self._on_tree_button_release, add="+")
        self.tree.bind("<Control-a>", self.select_all_shown_rows)
        self.tree.bind("<Control-A>", self.select_all_shown_rows)

        pager = ttk.Frame(self)
        pager.grid(row=5, column=0, sticky="ew", pady=(8, 0))
        pager.columnconfigure(2, weight=1)

        ttk.Button(pager, text="◀ Previous", command=self.prev_page).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(pager, text="Next ▶", command=self.next_page).grid(row=0, column=1, padx=(0, 12))

        self.page_label = ttk.Label(pager, text="Page 1 / 1")
        self.page_label.grid(row=0, column=2, sticky="w")

        ttk.Label(pager, text="Rows/page:").grid(row=0, column=3, sticky="e")
        self.page_size_var = tk.StringVar(value=str(self.state.page_size))
        page_size_combo = ttk.Combobox(
            pager,
            width=8,
            textvariable=self.page_size_var,
            values=("100", "200", "500", "1000"),
            state="readonly",
        )
        page_size_combo.grid(row=0, column=4, padx=(6, 0))
        page_size_combo.bind("<<ComboboxSelected>>", self._on_page_size_changed)

        status = ttk.Label(self, textvariable=self.status_var, anchor="w")
        status.grid(row=6, column=0, sticky="ew", pady=(8, 0))

    def open_existing_db(self) -> None:
        current_path = Path(self.db_path_var.get()).expanduser() if self.db_path_var.get().strip() else Path.cwd()
        initial_dir = str(current_path.parent if current_path.suffix else current_path)
        selected = filedialog.askopenfilename(
            title="Select SQLite database",
            filetypes=[("SQLite database", "*.db"), ("All files", "*.*")],
            initialdir=initial_dir,
            initialfile=current_path.name if current_path.suffix else "",
        )
        if selected:
            self.db_path_var.set(selected)
            self.open_database(selected)

    def create_new_db(self) -> None:
        current_path = Path(self.db_path_var.get()).expanduser() if self.db_path_var.get().strip() else Path.cwd() / 'metadata_index.db'
        selected = filedialog.asksaveasfilename(
            title="Create SQLite database",
            defaultextension=".db",
            filetypes=[("SQLite database", "*.db"), ("All files", "*.*")],
            initialdir=str(current_path.parent),
            initialfile=current_path.name if current_path.suffix else 'metadata_index.db',
        )
        if selected:
            self.db_path_var.set(selected)
            self.open_database(selected)

    def browse_html_dir(self) -> None:
        selected = filedialog.askdirectory(title="Select HTML directory", initialdir=self.html_dir_var.get().strip() or str(Path.cwd()))
        if selected:
            self.html_dir_var.set(selected)

    def _invalidate_result_cache(self) -> None:
        self._result_cache_signature = None
        self._result_cache_rows = None

    def _current_query_signature(self) -> tuple[object, ...]:
        normalized_rules = tuple(
            (
                str(rule.get("column", "")),
                str(rule.get("operator", "")),
                str(rule.get("value", "")),
                str(rule.get("unit", "B")),
            )
            for rule in (self.state.filter_rules or [])
        )
        return (
            self.state.search,
            self.state.search_mode,
            self.state.search_field,
            normalized_rules,
        )

    def _row_sort_key(self, row: sqlite3.Row, column: str) -> object:
        value = row[column]
        if column in {"id", "filesize_bytes"}:
            try:
                return int(value)
            except (TypeError, ValueError):
                return 0
        return str(value or "").casefold()

    def _get_cached_page_rows(self) -> list[sqlite3.Row]:
        rows = list(self._result_cache_rows or [])
        rows.sort(key=lambda row: int(row["id"]))
        rows.sort(key=lambda row: self._row_sort_key(row, self.state.sort_column), reverse=self.state.sort_desc)
        start = (self.state.page - 1) * self.state.page_size
        end = start + self.state.page_size
        return rows[start:end]

    def open_database(self, db_path: str) -> None:
        db_path = db_path.strip()
        if not db_path:
            messagebox.showinfo("No database selected", "Select an existing database or create a new one first.")
            self.status_var.set("No database opened. Select or create a SQLite DB to begin.")
            self._reset_table_view()
            return

        db_path = str(Path(db_path).expanduser())
        try:
            if self.conn is not None:
                self.conn.close()
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            self.conn = connect(db_path)
            init_db(self.conn)
            self._invalidate_result_cache()
            self.refresh_categories()
        except Exception as exc:
            self.conn = None
            self.refresh_categories()
            self._reset_table_view()
            messagebox.showerror("Database error", f"Could not open database:\n{exc}")
            self.status_var.set("Failed to open database.")
            return

        self.db_path_var.set(db_path)
        self.state.page = 1
        self.status_var.set(f"Database ready: {db_path}")
        self.load_page(reset_count=True)

    def refresh_categories(self) -> None:
        if self.conn is None:
            self.category_cache = []
            return
        self.category_cache = list_categories(self.conn)

    def _reset_table_view(self) -> None:
        self._invalidate_result_cache()
        self.tree.delete(*self.tree.get_children())
        self.state.total_rows = 0
        self.state.page = 1
        self.page_label.config(text="No database opened | Rows: 0")

    def _on_search_submitted(self, _event=None) -> None:
        self.apply_search()

    def _selected_search_field(self) -> str:
        label = self.search_field_var.get().strip() or "All fields"
        return SEARCH_SCOPE_LABEL_TO_VALUE.get(label, "all")

    def _on_search_mode_changed(self, _event=None) -> None:
        self.state.search_mode = self.search_mode_var.get().strip().lower() or "any"

    def _on_search_field_changed(self, _event=None) -> None:
        self.state.search_field = self._selected_search_field()

    def apply_search(self) -> None:
        self.state.search = self.search_var.get().strip()
        self.state.search_mode = self.search_mode_var.get().strip().lower() or "any"
        self.state.search_field = self._selected_search_field()
        self.state.page = 1
        self.load_page(reset_count=True)

    def refresh_results(self) -> None:
        self.state.search = self.search_var.get().strip()
        self.state.search_mode = self.search_mode_var.get().strip().lower() or "any"
        self.state.search_field = self._selected_search_field()
        self.load_page(reset_count=True)

    def clear_search(self) -> None:
        self.search_var.set("")
        self.search_mode_var.set("any")
        self.search_field_var.set("All fields")
        self.state.search = ""
        self.state.search_mode = "any"
        self.state.search_field = "all"
        self.state.page = 1
        self.load_page(reset_count=True)

    def open_advanced_filters(self) -> None:
        dialog = AdvancedFiltersDialog(self.master, initial_rules=list(self.state.filter_rules or []))
        self.master.wait_window(dialog)
        if dialog.result is None:
            return
        self.state.filter_rules = dialog.result
        self._refresh_filter_summary()
        self.state.page = 1
        self.load_page(reset_count=True)

    def clear_filters(self) -> None:
        self.state.filter_rules = []
        self._refresh_filter_summary()
        self.state.page = 1
        self.load_page(reset_count=True)

    def _refresh_filter_summary(self) -> None:
        rules = self.state.filter_rules or []
        if not rules:
            self.filter_summary_var.set("Filters: none")
            return
        pieces = [filter_rule_summary(rule) for rule in rules]
        self.filter_summary_var.set("Filters: " + " AND ".join(pieces))

    def _on_page_size_changed(self, _event=None) -> None:
        self.state.page_size = int(self.page_size_var.get())
        self.state.page = 1
        self.load_page(reset_count=True)

    def toggle_sort(self, column: SortColumn) -> None:
        if self.state.sort_column == column:
            self.state.sort_desc = not self.state.sort_desc
        else:
            self.state.sort_column = column
            self.state.sort_desc = False
        self.load_page(reset_count=False)

    def load_page(self, *, reset_count: bool) -> None:
        if self.conn is None:
            self._reset_table_view()
            return

        query_signature = self._current_query_signature()
        try:
            if reset_count:
                self.state.total_rows = count_records(
                    self.conn,
                    search=self.state.search,
                    search_mode=self.state.search_mode,
                    search_field=self.state.search_field,
                    filter_rules=self.state.filter_rules or [],
                )
                if self.state.total_rows <= self.result_cache_max_rows:
                    self._result_cache_rows = fetch_records(
                        self.conn,
                        search=self.state.search,
                        search_mode=self.state.search_mode,
                        search_field=self.state.search_field,
                        sort_column="id",
                        sort_desc=False,
                        limit=max(self.state.total_rows, 1),
                        offset=0,
                        filter_rules=self.state.filter_rules or [],
                    )
                    self._result_cache_signature = query_signature
                else:
                    self._invalidate_result_cache()

            total_pages = max(1, math.ceil(self.state.total_rows / self.state.page_size))
            self.state.page = max(1, min(self.state.page, total_pages))
            offset = (self.state.page - 1) * self.state.page_size

            if self._result_cache_signature == query_signature and self._result_cache_rows is not None:
                rows = self._get_cached_page_rows()
            else:
                rows = fetch_records(
                    self.conn,
                    search=self.state.search,
                    search_mode=self.state.search_mode,
                    search_field=self.state.search_field,
                    sort_column=self.state.sort_column,
                    sort_desc=self.state.sort_desc,
                    limit=self.state.page_size,
                    offset=offset,
                    filter_rules=self.state.filter_rules or [],
                )
        except ValueError as exc:
            messagebox.showerror("Filter error", str(exc))
            return

        self.tree.delete(*self.tree.get_children())
        for row in rows:
            self.tree.insert(
                "",
                "end",
                iid=str(row["id"]),
                values=(
                    row["id"],
                    row["category"] or "",
                    row["source_title"] or "",
                    row["title"],
                    format_filesize(row["filesize_bytes"]),
                    row["info_hash"],
                    row["resource_link"],
                ),
            )

        arrow = "▼" if self.state.sort_desc else "▲"
        filter_text = self._current_filter_summary()
        self.page_label.config(
            text=(
                f"Page {self.state.page} / {total_pages}  |  "
                f"Rows: {self.state.total_rows}  |  "
                f"Search: {self._current_search_summary()}  |  "
                f"Filter: {filter_text}  |  "
                f"Sort: {COLUMN_HEADINGS[self.state.sort_column]} {arrow}"
            )
        )

    def _current_search_summary(self) -> str:
        if not self.state.search:
            return "none"
        field_label = SEARCH_SCOPE_VALUE_TO_LABEL.get(self.state.search_field, self.state.search_field)
        return f"{self.state.search_mode} words in {field_label}: {self.state.search}"

    def _current_filter_summary(self) -> str:
        rules = self.state.filter_rules or []
        if not rules:
            return "none"
        return " AND ".join(filter_rule_summary(rule) for rule in rules)

    def get_selected_ids(self) -> list[int]:
        return [int(item_id) for item_id in self.tree.selection()]

    def add_row(self) -> None:
        if self.conn is None:
            return
        dialog = RecordDialog(self.master, "Add metadata entry", self.category_cache)
        self.master.wait_window(dialog)
        if not dialog.result:
            return
        try:
            add_record(
                self.conn,
                dialog.result["title"],
                dialog.result["category"],
                dialog.result["source_title"],
                int(dialog.result["filesize_bytes"]),
                dialog.result["info_hash"],
            )
        except sqlite3.IntegrityError as exc:
            messagebox.showerror("Insert failed", f"Could not insert row:\n{exc}")
            return
        self.refresh_categories()
        self._invalidate_result_cache()
        self.state.page = 1
        self.status_var.set("Row inserted.")
        self.load_page(reset_count=True)

    def edit_selected(self) -> None:
        if self.conn is None:
            return
        selected_ids = self.get_selected_ids()
        if not selected_ids:
            messagebox.showinfo("No selection", "Select a row first.")
            return
        if len(selected_ids) > 1:
            messagebox.showinfo("Multiple selection", "Modify works on one row at a time.")
            return

        record_id = selected_ids[0]
        record = get_record_by_id(self.conn, record_id)
        if record is None:
            messagebox.showerror("Modify failed", "The selected row no longer exists.")
            self.load_page(reset_count=True)
            return
        dialog = RecordDialog(
            self.master,
            "Modify metadata entry",
            self.category_cache,
            initial={
                "category": str(record["category"] or ""),
                "source_title": str(record["source_title"] or ""),
                "title": str(record["title"]),
                "filesize_bytes": str(record["filesize_bytes"]),
                "info_hash": str(record["info_hash"]),
            },
        )
        self.master.wait_window(dialog)
        if not dialog.result:
            return
        try:
            update_record(
                self.conn,
                record_id,
                dialog.result["title"],
                dialog.result["category"],
                dialog.result["source_title"],
                int(dialog.result["filesize_bytes"]),
                dialog.result["info_hash"],
            )
        except sqlite3.IntegrityError as exc:
            messagebox.showerror("Update failed", f"Could not update row:\n{exc}")
            return
        self.refresh_categories()
        self._invalidate_result_cache()
        self.status_var.set("Row updated.")
        self.load_page(reset_count=True)

    def delete_selected(self) -> None:
        if self.conn is None:
            return
        selected_ids = self.get_selected_ids()
        if not selected_ids:
            messagebox.showinfo("No selection", "Select one or more rows first.")
            return
        if not messagebox.askyesno("Confirm delete", f"Delete {len(selected_ids)} selected row(s)?"):
            return
        for record_id in selected_ids:
            delete_record(self.conn, record_id)
        self._invalidate_result_cache()
        self.status_var.set(f"Deleted {len(selected_ids)} row(s).")
        self.load_page(reset_count=True)

    def manage_categories(self) -> None:
        if self.conn is None:
            return
        dialog = CategoryManagerDialog(self.master, self.conn)
        self.master.wait_window(dialog)
        self.refresh_categories()
        self.load_page(reset_count=True)

    def assign_category_to_selection(self) -> None:
        if self.conn is None:
            return
        selected_ids = self.get_selected_ids()
        if not selected_ids:
            messagebox.showinfo("No selection", "Select one or more rows first.")
            return
        dialog = AssignCategoryDialog(self.master, self.category_cache)
        self.master.wait_window(dialog)
        if dialog.result is None:
            return
        updated = assign_category(self.conn, selected_ids, dialog.result)
        self.refresh_categories()
        self._invalidate_result_cache()
        category_label = dialog.result or "(empty)"
        self.status_var.set(f"Assigned {category_label} to {updated} row(s).")
        self.load_page(reset_count=True)

    def start_import(self) -> None:
        if self.import_thread and self.import_thread.is_alive():
            messagebox.showinfo("Import running", "An import is already in progress.")
            return

        db_path_text = self.db_path_var.get().strip()
        html_dir_text = self.html_dir_var.get().strip()
        if not db_path_text:
            messagebox.showerror("No database selected", "Select an existing database or create a new one first.")
            return
        if not html_dir_text:
            messagebox.showerror("No HTML folder selected", "Select an HTML directory first.")
            return

        db_path = Path(db_path_text).expanduser()
        html_dir = Path(html_dir_text).expanduser()
        if not html_dir.exists() or not html_dir.is_dir():
            messagebox.showerror("Invalid folder", "Select a valid HTML directory first.")
            return
        if self.conn is None:
            self.open_database(str(db_path))
            if self.conn is None:
                return

        error_log = db_path.with_name(f"{db_path.stem}_import_errors.tsv")
        self.import_button.state(["disabled"])
        self.status_var.set(f"Importing HTML from {html_dir} ... 0 files processed")
        self.import_result = None
        self._drain_import_progress_queue()

        def publish_progress(progress: dict[str, int]) -> None:
            self.import_progress_queue.put(progress)

        def worker() -> None:
            try:
                summary = index_directory(html_dir, db_path, error_log=error_log, progress_callback=publish_progress)
                self.import_result = (
                    True,
                    f"Import complete | files: {summary['files_processed']} / {summary.get('total_files', summary['files_processed'])} | rows imported: {summary['rows_imported']} | errors: {summary['errors']} | log: {error_log}",
                )
            except Exception as exc:
                self.import_result = (False, f"Import failed: {exc}")

        self.import_thread = threading.Thread(target=worker, daemon=True)
        self.import_thread.start()
        self.after(150, self._poll_import_thread)

    def _poll_import_thread(self) -> None:
        self._drain_import_progress_queue()
        if self.import_thread and self.import_thread.is_alive():
            self.after(150, self._poll_import_thread)
            return

        self._drain_import_progress_queue()
        self.import_button.state(["!disabled"])
        result = self.import_result
        if result is None:
            self.status_var.set("Import finished, but no summary was returned.")
            return

        ok, message = result
        self._invalidate_result_cache()
        self.open_database(self.db_path_var.get())
        self.status_var.set(message)
        if not ok:
            messagebox.showerror("Import failed", message)

    def _drain_import_progress_queue(self) -> None:
        latest: dict[str, int] | None = None
        while True:
            try:
                latest = self.import_progress_queue.get_nowait()
            except queue.Empty:
                break
        if latest is None:
            return
        processed = latest.get("files_processed", 0)
        total = latest.get("total_files", 0)
        rows_imported = latest.get("rows_imported", 0)
        errors = latest.get("errors", 0)
        if total > 0:
            self.status_var.set(
                f"Importing HTML... files processed: {processed} / {total} | rows imported: {rows_imported} | errors: {errors}"
            )
        else:
            self.status_var.set(
                f"Importing HTML... files processed: {processed} | rows imported: {rows_imported} | errors: {errors}"
            )

    def select_all_shown_rows(self, _event=None) -> str:
        visible_ids = self.tree.get_children("")
        if not visible_ids:
            self.status_var.set("No shown rows to select.")
            return "break"
        self.tree.selection_set(visible_ids)
        self.tree.focus(visible_ids[0])
        self.tree.see(visible_ids[0])
        self.status_var.set(f"Selected {len(visible_ids)} shown row(s).")
        return "break"

    def _selected_item_ids_in_view_order(self) -> list[str]:
        selected = set(self.tree.selection())
        return [item_id for item_id in self.tree.get_children("") if item_id in selected]

    def _copy_selected_field(self, value_index: int, singular_label: str, plural_label: str) -> None:
        selected_item_ids = self._selected_item_ids_in_view_order()
        if not selected_item_ids:
            messagebox.showinfo("No selection", "Select one or more rows first.")
            return
        values_to_copy: list[str] = []
        for item_id in selected_item_ids:
            row_values = self.tree.item(item_id, "values")
            if value_index >= len(row_values):
                continue
            value = str(row_values[value_index]).strip()
            if value:
                values_to_copy.append(value)
        if not values_to_copy:
            messagebox.showinfo("Nothing to copy", f"No non-empty {plural_label.lower()} were found in the selection.")
            return
        clipboard_text = ", ".join(values_to_copy)
        self.master.clipboard_clear()
        self.master.clipboard_append(clipboard_text)
        self.master.update_idletasks()
        label = singular_label if len(values_to_copy) == 1 else plural_label
        self.status_var.set(f"Copied {len(values_to_copy)} {label.lower()} to the clipboard.")

    def copy_selected_link(self) -> None:
        self._copy_selected_field(6, "link", "links")

    def copy_selected_source_titles(self) -> None:
        self._copy_selected_field(2, "source title", "source titles")

    def copy_selected_titles(self) -> None:
        self._copy_selected_field(3, "title", "titles")

    def _rebuild_row_menu(self) -> None:
        self.row_menu = tk.Menu(self, tearoff=False)
        self.row_menu.add_command(label="Copy Selected Link(s)", command=self.copy_selected_link)
        self.row_menu.add_command(label="Copy Selected Source Title(s)", command=self.copy_selected_source_titles)
        self.row_menu.add_command(label="Copy Selected Title(s)", command=self.copy_selected_titles)
        self.row_menu.add_separator()
        self.row_menu.add_command(label="Delete Selected Row(s)", command=self.delete_selected)

    def _on_tree_right_click(self, event: tk.Event) -> None:
        region = self.tree.identify_region(event.x, event.y)
        if region == "heading":
            self._show_column_menu(event)
            return

        row_id = self.tree.identify_row(event.y)
        if row_id:
            if row_id not in self.tree.selection():
                self.tree.selection_set(row_id)
            self.tree.focus(row_id)
            if self.row_menu is None:
                self._rebuild_row_menu()
            assert self.row_menu is not None
            self.row_menu.tk_popup(event.x_root, event.y_root)
            return

        self._show_column_menu(event)

    def _rebuild_column_menu(self) -> None:
        self.column_menu = tk.Menu(self, tearoff=False)
        self.column_visibility_vars = {}
        for column in ALL_COLUMNS:
            var = tk.BooleanVar(value=(column in self.visible_columns))
            self.column_visibility_vars[column] = var
            self.column_menu.add_checkbutton(
                label=COLUMN_HEADINGS[column],
                onvalue=True,
                offvalue=False,
                variable=var,
                command=lambda c=column: self.toggle_column_visibility(c),
            )
        self.column_menu.add_separator()
        self.column_menu.add_command(label="Show All Columns", command=self.show_all_columns)
        self.column_menu.add_command(label="Reset Column Order", command=self.reset_column_order)

    def _show_column_menu(self, event: tk.Event) -> None:
        if self.column_menu is None:
            self._rebuild_column_menu()
        self._refresh_filter_summary()
        # refresh check states on every open
        self._rebuild_column_menu()
        self._refresh_filter_summary()
        assert self.column_menu is not None
        self.column_menu.tk_popup(event.x_root, event.y_root)

    def toggle_column_visibility(self, column: str) -> None:
        if column in self.visible_columns:
            if len(self.visible_columns) == 1:
                messagebox.showinfo("Columns", "At least one column must remain visible.")
                if column in self.column_visibility_vars:
                    self.column_visibility_vars[column].set(True)
                return
            self.visible_columns.remove(column)
        else:
            self.visible_columns.add(column)
        self._apply_display_columns()

    def show_all_columns(self) -> None:
        self.visible_columns = set(ALL_COLUMNS)
        self._apply_display_columns()

    def reset_column_order(self) -> None:
        self.column_order = list(ALL_COLUMNS)
        self.visible_columns = set(ALL_COLUMNS)
        self._apply_display_columns()

    def _apply_display_columns(self) -> None:
        display_columns = [column for column in self.column_order if column in self.visible_columns]
        if not display_columns:
            display_columns = [self.column_order[0]]
            self.visible_columns.add(self.column_order[0])
        self.tree.configure(displaycolumns=display_columns)

    def _display_columns(self) -> list[str]:
        current = self.tree.cget("displaycolumns")
        if isinstance(current, str):
            if current == "#all":
                return list(ALL_COLUMNS)
            return [c.strip() for c in current.split() if c.strip()]
        return list(current)

    def _event_column_name(self, event_x: int) -> str | None:
        identified = self.tree.identify_column(event_x)
        if not identified or not identified.startswith("#"):
            return None
        try:
            index = int(identified[1:]) - 1
        except ValueError:
            return None
        display_columns = self._display_columns()
        if 0 <= index < len(display_columns):
            return display_columns[index]
        return None

    def _on_tree_button_press(self, event: tk.Event) -> None:
        region = self.tree.identify_region(event.x, event.y)
        if region == "heading":
            self.drag_source_column = self._event_column_name(event.x)
        else:
            self.drag_source_column = None

    def _on_tree_button_release(self, event: tk.Event) -> None:
        if not self.drag_source_column:
            return
        region = self.tree.identify_region(event.x, event.y)
        target_column = self._event_column_name(event.x) if region == "heading" else None
        source_column = self.drag_source_column
        self.drag_source_column = None
        if not target_column or source_column == target_column:
            return

        visible_order = self._display_columns()
        if source_column not in visible_order or target_column not in visible_order:
            return
        visible_order.remove(source_column)
        target_index = visible_order.index(target_column)
        visible_order.insert(target_index, source_column)

        hidden_order = [column for column in self.column_order if column not in visible_order]
        self.column_order = visible_order + hidden_order
        self._apply_display_columns()
        self.status_var.set("Columns reordered.")

    def prev_page(self) -> None:
        if self.state.page > 1:
            self.state.page -= 1
            self.load_page(reset_count=False)

    def next_page(self) -> None:
        total_pages = max(1, math.ceil(self.state.total_rows / self.state.page_size))
        if self.state.page < total_pages:
            self.state.page += 1
            self.load_page(reset_count=False)

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
        self.master.destroy()


def launch_gui(db_path: str | None = None) -> None:
    root = tk.Tk()
    app = MetadataManagerApp(root, db_path)
    root.protocol("WM_DELETE_WINDOW", app.close)
    root.mainloop()
