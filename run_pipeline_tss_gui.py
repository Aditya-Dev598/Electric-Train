import importlib.util
import os
import traceback

if importlib.util.find_spec("_tkinter") is None:
    raise SystemExit(
        "Tkinter is not available in this Python build.\n"
        "Install a Tk-enabled Python and rerun.\n\n"
        "macOS (Homebrew):\n"
        "  brew install tcl-tk\n"
        "  brew install python-tk@3.12  # or matching Python version\n\n"
        "Ubuntu/Debian:\n"
        "  sudo apt-get install python3-tk\n"
    )

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

import pandas as pd

from run_pipeline_tss_assigned import (
    aggregate_half_hour,
    expand_services,
    load_or_make_energy_params,
    safe_name,
)


class PipelineGui(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("TSS Energy Pipeline GUI")
        self.geometry("1300x800")

        self.simple_timetable_var = tk.StringVar()
        self.route_template_var = tk.StringVar()
        self.stations_var = tk.StringVar()
        self.energy_params_var = tk.StringVar()
        self.outdir_var = tk.StringVar(value=os.path.join(os.getcwd(), "gui_output"))
        self.regen_eff_var = tk.StringVar(value="0.25")
        self.split_cross_tss_var = tk.StringVar(value="0.5")

        self.output_frames = []

        self._build_ui()

    def _build_ui(self):
        container = ttk.Frame(self, padding=12)
        container.pack(fill=tk.BOTH, expand=True)

        inputs = ttk.LabelFrame(container, text="Input files & options", padding=10)
        inputs.pack(fill=tk.X)

        self._file_row(inputs, 0, "Simple timetable CSV", self.simple_timetable_var)
        self._file_row(inputs, 1, "Route template CSV", self.route_template_var)
        self._file_row(inputs, 2, "Stations CSV", self.stations_var)
        self._file_row(inputs, 3, "Energy params CSV (optional)", self.energy_params_var, required=False)

        self._folder_row(inputs, 4, "Output directory", self.outdir_var)

        ttk.Label(inputs, text="Regen efficiency (0-1)").grid(row=5, column=0, sticky="w", padx=(0, 8), pady=6)
        ttk.Entry(inputs, textvariable=self.regen_eff_var, width=16).grid(row=5, column=1, sticky="w", pady=6)

        ttk.Label(inputs, text="Split cross-TSS (0-1)").grid(row=5, column=2, sticky="w", padx=(20, 8), pady=6)
        ttk.Entry(inputs, textvariable=self.split_cross_tss_var, width=16).grid(row=5, column=3, sticky="w", pady=6)

        actions = ttk.Frame(container)
        actions.pack(fill=tk.X, pady=(10, 8))

        ttk.Button(actions, text="Run pipeline", command=self.run_pipeline).pack(side=tk.LEFT)
        ttk.Button(actions, text="Open output folder", command=self.open_output_folder).pack(side=tk.LEFT, padx=(8, 0))

        self.status_var = tk.StringVar(value="Select your files and click 'Run pipeline'.")
        ttk.Label(container, textvariable=self.status_var).pack(fill=tk.X)

        self.notebook = ttk.Notebook(container)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=(8, 0))

    def _file_row(self, parent, row, label, var, required=True):
        required_suffix = " *" if required else ""
        ttk.Label(parent, text=label + required_suffix).grid(row=row, column=0, sticky="w", padx=(0, 8), pady=6)
        ttk.Entry(parent, textvariable=var, width=90).grid(row=row, column=1, columnspan=3, sticky="ew", pady=6)
        ttk.Button(parent, text="Browse", command=lambda: self._pick_file(var)).grid(row=row, column=4, padx=(8, 0), pady=6)

    def _folder_row(self, parent, row, label, var):
        ttk.Label(parent, text=label + " *").grid(row=row, column=0, sticky="w", padx=(0, 8), pady=6)
        ttk.Entry(parent, textvariable=var, width=90).grid(row=row, column=1, columnspan=3, sticky="ew", pady=6)
        ttk.Button(parent, text="Browse", command=lambda: self._pick_folder(var)).grid(row=row, column=4, padx=(8, 0), pady=6)

    @staticmethod
    def _pick_file(var):
        path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
        if path:
            var.set(path)

    @staticmethod
    def _pick_folder(var):
        path = filedialog.askdirectory()
        if path:
            var.set(path)

    def _clear_output_tabs(self):
        for frame in self.output_frames:
            frame.destroy()
        self.output_frames.clear()

    def _show_dataframe_in_tab(self, tab_name, df):
        frame = ttk.Frame(self.notebook)
        self.output_frames.append(frame)
        self.notebook.add(frame, text=tab_name)

        table_frame = ttk.Frame(frame)
        table_frame.pack(fill=tk.BOTH, expand=True)

        cols = list(df.columns)
        tree = ttk.Treeview(table_frame, columns=cols, show="headings")
        y_scroll = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=tree.yview)
        x_scroll = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)

        tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")

        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        for col in cols:
            tree.heading(col, text=col)
            tree.column(col, width=120, stretch=True)

        for _, row in df.iterrows():
            tree.insert("", tk.END, values=[row[c] for c in cols])

    def _validate_inputs(self):
        required_files = [
            ("Simple timetable CSV", self.simple_timetable_var.get().strip()),
            ("Route template CSV", self.route_template_var.get().strip()),
            ("Stations CSV", self.stations_var.get().strip()),
        ]

        for label, path in required_files:
            if not path:
                raise ValueError(f"{label} is required.")
            if not os.path.isfile(path):
                raise ValueError(f"{label} not found: {path}")

        energy_params_path = self.energy_params_var.get().strip()
        if energy_params_path and not os.path.isfile(energy_params_path):
            raise ValueError(f"Energy params CSV not found: {energy_params_path}")

        outdir = self.outdir_var.get().strip()
        if not outdir:
            raise ValueError("Output directory is required.")

        try:
            regen_eff = float(self.regen_eff_var.get())
            split_cross_tss = float(self.split_cross_tss_var.get())
        except ValueError as exc:
            raise ValueError("Regen efficiency and split cross-TSS must be numeric values.") from exc

        return energy_params_path or None, outdir, regen_eff, split_cross_tss

    def run_pipeline(self):
        try:
            energy_params_path, outdir, regen_eff, split_cross_tss = self._validate_inputs()

            self.status_var.set("Loading input files...")
            self.update_idletasks()

            tt = pd.read_csv(self.simple_timetable_var.get().strip())
            route = pd.read_csv(self.route_template_var.get().strip())
            stations = pd.read_csv(self.stations_var.get().strip())
            energy_params = load_or_make_energy_params(
                energy_params_path,
                tt["train_type"].astype(str).tolist(),
            )

            self.status_var.set("Running pipeline...")
            self.update_idletasks()

            events = expand_services(
                tt=tt,
                route=route,
                stations=stations,
                energy_params=energy_params,
                split_cross_tss=split_cross_tss,
                regen_eff_default=regen_eff,
            )

            if "missing_tss" in events.columns:
                missing_rows = events[events["missing_tss"] == True]
                if not missing_rows.empty:
                    os.makedirs(outdir, exist_ok=True)
                    missing_rows.to_csv(os.path.join(outdir, "DEBUG_missing_tss_rows.csv"), index=False)

            out_by_tss = aggregate_half_hour(events)
            if not out_by_tss:
                messagebox.showwarning(
                    "No output",
                    "No outputs produced. Check route_variant joins and station TSS assignments.",
                )
                self.status_var.set("No outputs produced.")
                return

            os.makedirs(outdir, exist_ok=True)
            self._clear_output_tabs()

            for tss, df in out_by_tss.items():
                output_name = safe_name(tss) + ".csv"
                output_path = os.path.join(outdir, output_name)
                df.to_csv(output_path, index=False)
                self._show_dataframe_in_tab(str(tss), df)

            self.status_var.set(f"Done. Wrote {len(out_by_tss)} files to {outdir}")
            messagebox.showinfo("Success", f"Pipeline complete. Wrote {len(out_by_tss)} files to:\n{outdir}")

        except Exception as exc:
            self.status_var.set("Pipeline failed. See error dialog.")
            messagebox.showerror("Error", f"{exc}\n\nDetails:\n{traceback.format_exc()}")

    def open_output_folder(self):
        outdir = self.outdir_var.get().strip()
        if not outdir:
            messagebox.showwarning("Output folder", "Set an output folder first.")
            return

        if not os.path.isdir(outdir):
            messagebox.showwarning("Output folder", f"Folder does not exist yet:\n{outdir}")
            return

        if os.name == "nt":
            os.startfile(outdir)
        else:
            messagebox.showinfo("Output folder", f"Output folder:\n{outdir}")


if __name__ == "__main__":
    app = PipelineGui()
    app.mainloop()
