import re
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

import matplotlib.pyplot as plt
import pandas as pd

TIME_RE = re.compile(r"^\s*(\d{1,2})[:.](\d{2})(?:[:.](\d{2}))?\s*$")


def parse_time_to_hhmm(col) -> str | None:
    if hasattr(col, "hour") and hasattr(col, "minute"):
        return f"{col.hour:02d}:{col.minute:02d}"

    if isinstance(col, pd.Timedelta):
        total_seconds = int(col.total_seconds())
        h = (total_seconds // 3600) % 24
        m = (total_seconds % 3600) // 60
        return f"{h:02d}:{m:02d}"

    if isinstance(col, str):
        m = TIME_RE.match(col.strip())
        if m:
            hh = int(m.group(1))
            mm = int(m.group(2))
            return f"{hh:02d}:{mm:02d}"

    return None


def parse_hour_label(col) -> str | None:
    hhmm = parse_time_to_hhmm(col)
    if hhmm and hhmm.endswith(":00"):
        return hhmm
    return None


def read_table(path: str) -> pd.DataFrame:
    ext = Path(path).suffix.lower()
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path)
    if ext == ".csv":
        return pd.read_csv(path)
    raise ValueError("Input must be .csv or .xlsx/.xls")


def write_table(df: pd.DataFrame, path: str):
    ext = Path(path).suffix.lower()
    if ext == ".csv":
        df.to_csv(path, index=False)
    elif ext in [".xlsx", ".xls"]:
        df.to_excel(path, index=False)
    else:
        raise ValueError("Output must be .csv or .xlsx/.xls")


def parse_date_series(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
    if dt.isna().mean() > 0.2:
        dt2 = pd.to_datetime(s, errors="coerce")
        dt = dt.fillna(dt2)
    return dt


def normalize_hour_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename = {}
    for c in df.columns:
        lab = parse_hour_label(c)
        if lab is not None:
            rename[c] = lab
    out = df.copy().rename(columns=rename)
    out.columns = [str(c).strip() for c in out.columns]
    return out


def ensure_24_hours(df: pd.DataFrame, who: str) -> list[str]:
    hour_cols = [f"{h:02d}:00" for h in range(24)]
    missing = [c for c in hour_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{who}: missing hour columns after normalization: {missing}")
    return hour_cols


def hh_wide_to_hourly_wide_no_day(input_path: str) -> pd.DataFrame:
    df = read_table(input_path)

    date_col = None
    for c in df.columns:
        if isinstance(c, str) and "date" in c.lower():
            date_col = c
            break
    if date_col is None:
        date_col = df.columns[0]

    time_map = {}
    for c in df.columns:
        if c == date_col:
            continue
        hhmm = parse_time_to_hhmm(c)
        if hhmm is not None:
            time_map[c] = hhmm

    if len(time_map) < 40:
        raise ValueError(
            f"Could not detect HH time columns properly (found {len(time_map)})."
        )

    hh = df[[date_col] + list(time_map.keys())].rename(columns=time_map).copy()
    hh[date_col] = pd.to_datetime(hh[date_col], dayfirst=True, errors="coerce")
    if hh[date_col].isna().mean() > 0.2:
        raise ValueError(f"Date parsing failed for column '{date_col}'.")

    for c in hh.columns:
        if c != date_col:
            hh[c] = pd.to_numeric(hh[c], errors="coerce").fillna(0)

    out = pd.DataFrame()
    out["Date"] = hh[date_col].dt.strftime("%d/%m/%Y")

    hour_cols = []
    for h in range(24):
        t00 = f"{h:02d}:00"
        t30 = f"{h:02d}:30"
        out[t00] = (hh[t00] if t00 in hh.columns else 0) + (hh[t30] if t30 in hh.columns else 0)
        hour_cols.append(t00)

    out["Total Units"] = out[hour_cols].sum(axis=1)
    return out[["Date", "Total Units"] + hour_cols]


def clean_pvgis_to_wide_hourly(input_path: str) -> pd.DataFrame:
    header_row = None
    with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f):
            if line.strip().lower().startswith("time,"):
                header_row = i
                break

    if header_row is None:
        raise ValueError("Could not find PVGIS table header (line starting with 'time,').")

    df = pd.read_csv(input_path, skiprows=header_row)
    if "time" not in df.columns or "P" not in df.columns:
        raise ValueError(f"Expected columns 'time' and 'P'. Found: {df.columns.tolist()}")

    dt = pd.to_datetime(df["time"].astype(str).str.strip(), format="%Y%m%d:%H%M", errors="coerce")
    if dt.isna().mean() > 0.1:
        raise ValueError("Failed to parse PVGIS 'time' column with format %Y%m%d:%H%M.")

    out = pd.DataFrame({"datetime": dt, "kW": pd.to_numeric(df["P"], errors="coerce").fillna(0) / 1000.0})
    out["datetime"] = out["datetime"].dt.floor("h")
    out = out.groupby("datetime", as_index=False)["kW"].mean()
    out["DateKey"] = out["datetime"].dt.normalize()
    out["Hour"] = out["datetime"].dt.strftime("%H:%M")

    wide = out.pivot_table(index="DateKey", columns="Hour", values="kW", aggfunc="mean")
    hour_cols = [f"{h:02d}:00" for h in range(24)]
    wide = wide.reindex(columns=hour_cols).fillna(0)

    final = pd.DataFrame()
    final["Date"] = wide.index.strftime("%d/%m/%Y")
    final["Total Units"] = wide[hour_cols].sum(axis=1)
    for c in hour_cols:
        final[c] = wide[c].values
    return final[["Date", "Total Units"] + hour_cols]


def build_supply_md_latest(supply_df: pd.DataFrame, hour_cols: list[str]) -> pd.DataFrame:
    supply = supply_df[["Date"] + hour_cols].copy()
    supply["Date_dt"] = parse_date_series(supply["Date"])
    if supply["Date_dt"].isna().mean() > 0.2:
        raise ValueError("Could not parse supply Date reliably.")

    supply["md"] = supply["Date_dt"].dt.strftime("%m-%d")
    for c in hour_cols:
        supply[c] = pd.to_numeric(supply[c], errors="coerce").fillna(0)

    supply["_year"] = supply["Date_dt"].dt.year
    return (
        supply.sort_values(["md", "_year"], ascending=[True, False])
        .drop_duplicates("md", keep="first")
        .drop(columns=["_year"])
    )


def match_supply_to_demand_by_md_latest(
    demand_df: pd.DataFrame, supply_df: pd.DataFrame, hour_cols: list[str]
) -> pd.DataFrame:
    demand = demand_df[["Date"] + hour_cols].copy()
    demand["Date_dt"] = parse_date_series(demand["Date"])
    if demand["Date_dt"].isna().mean() > 0.2:
        raise ValueError("Could not parse demand Date reliably.")
    demand["md"] = demand["Date_dt"].dt.strftime("%m-%d")

    for c in hour_cols:
        demand[c] = pd.to_numeric(demand[c], errors="coerce").fillna(0)

    supply_md = build_supply_md_latest(supply_df, hour_cols)
    merged = demand.merge(
        supply_md[["md"] + hour_cols],
        on="md",
        how="left",
        suffixes=("_demand", "_supply"),
    )

    supply_cols = [f"{c}_supply" for c in hour_cols]
    if merged[supply_cols].isna().all(axis=1).any():
        missing = merged.loc[merged[supply_cols].isna().all(axis=1), "md"].unique().tolist()
        raise ValueError(f"Supply missing matches for MM-DD keys: {missing[:25]}")

    merged[supply_cols] = merged[supply_cols].fillna(0)
    return merged


def build_average_profile_and_plot(demand_path: str, supply_path: str, output_png: str, output_xlsx: str):
    demand_df = normalize_hour_columns(read_table(demand_path))
    supply_df = normalize_hour_columns(read_table(supply_path))

    if "Date" not in demand_df.columns or "Date" not in supply_df.columns:
        raise ValueError("Both files must have a 'Date' column.")

    hour_cols = ensure_24_hours(demand_df, "DEMAND")
    ensure_24_hours(supply_df, "SUPPLY")

    merged = match_supply_to_demand_by_md_latest(demand_df, supply_df, hour_cols)

    avg_demand = []
    avg_supply = []
    used_solar_avg = []
    for c in hour_cols:
        d = merged[f"{c}_demand"].mean()
        s = merged[f"{c}_supply"].mean()
        avg_demand.append(d)
        avg_supply.append(s)
        used_solar_avg.append(min(d, s))

    profile = pd.DataFrame(
        {
            "Hour": hour_cols,
            "Average Demand": avg_demand,
            "Average Supply": avg_supply,
            "Used Solar (min of averages)": used_solar_avg,
        }
    )
    profile.to_excel(output_xlsx, index=False)

    x = list(range(24))
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(x, profile["Average Demand"], label="Average Demand", color="#e22a87", linewidth=2.5)
    ax.plot(x, profile["Average Supply"], label="Average Supply", color="#1e75bb", linewidth=2.5)
    ax.fill_between(x, profile["Used Solar (min of averages)"], color="#ffe784", alpha=0.6, label="Used Solar")
    ax.set_xlabel("Hour")
    ax.set_ylabel("P (same units as your files)")
    ax.set_xticks(x)
    ax.set_xticklabels(hour_cols, rotation=45, ha="right")
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.4)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(loc="upper left")
    plt.tight_layout()
    plt.savefig(output_png, dpi=300)
    plt.close()


def season_from_month(month: int) -> str:
    if month in (12, 1, 2):
        return "DJF"
    if month in (6, 7, 8):
        return "JJA"
    return "SHOULDER"


def compute_metrics_match_by_monthday_latest(demand_df: pd.DataFrame, supply_df: pd.DataFrame):
    demand_df = normalize_hour_columns(demand_df)
    supply_df = normalize_hour_columns(supply_df)

    hour_cols = [f"{h:02d}:00" for h in range(24)]
    if "Date" not in demand_df.columns or "Date" not in supply_df.columns:
        raise ValueError("Both files must have a 'Date' column.")

    demand = demand_df[["Date"] + hour_cols].copy()
    supply = supply_df[["Date"] + hour_cols].copy()
    demand["Date_dt"] = parse_date_series(demand["Date"])
    supply["Date_dt"] = parse_date_series(supply["Date"])

    if demand["Date_dt"].isna().mean() > 0.2:
        raise ValueError("Could not parse demand Date reliably.")
    if supply["Date_dt"].isna().mean() > 0.2:
        raise ValueError("Could not parse supply Date reliably.")

    demand["md"] = demand["Date_dt"].dt.strftime("%m-%d")
    supply["md"] = supply["Date_dt"].dt.strftime("%m-%d")

    for c in hour_cols:
        demand[c] = pd.to_numeric(demand[c], errors="coerce").fillna(0)
        supply[c] = pd.to_numeric(supply[c], errors="coerce").fillna(0)

    supply["_year"] = supply["Date_dt"].dt.year
    supply = (
        supply.sort_values(["md", "_year"], ascending=[True, False])
        .drop_duplicates("md", keep="first")
        .drop(columns=["_year"])
    )

    demand = demand.rename(columns={c: f"{c}_demand" for c in hour_cols})
    merged = demand.merge(supply[["md"] + hour_cols], on="md", how="left")
    supply_nan_mask = merged[hour_cols].isna().all(axis=1)
    if supply_nan_mask.any():
        missing_md = merged.loc[supply_nan_mask, "md"].unique().tolist()
        raise ValueError(f"No supply match found for month-days: {missing_md[:25]}")

    merged = merged.rename(columns={c: f"{c}_supply" for c in hour_cols})
    used_cols = []
    for c in hour_cols:
        u = f"{c}_used"
        merged[u] = merged[[f"{c}_demand", f"{c}_supply"]].min(axis=1)
        used_cols.append(u)

    total_demand = merged[[f"{c}_demand" for c in hour_cols]].to_numpy().sum()
    total_supply = merged[[f"{c}_supply" for c in hour_cols]].to_numpy().sum()
    used_solar = merged[used_cols].to_numpy().sum()

    annual_summary = pd.DataFrame(
        [
            {
                "Demand Days (rows)": len(merged),
                "Supply Days Used (unique md)": merged["md"].nunique(),
                "Total Demand": total_demand,
                "Total PV Supply (matched)": total_supply,
                "Used Solar": used_solar,
                "Spillage": total_supply - used_solar,
                "Solar Share (%)": (used_solar / total_demand) * 100 if total_demand > 0 else 0.0,
                "Utilisation (%)": (used_solar / total_supply) * 100 if total_supply > 0 else 0.0,
            }
        ]
    )

    merged["Season"] = merged["Date_dt"].dt.month.apply(season_from_month)
    seasonal_rows = []
    for season in ["DJF", "JJA", "SHOULDER"]:
        sub = merged[merged["Season"] == season]
        d = sub[[f"{c}_demand" for c in hour_cols]].to_numpy().sum()
        s = sub[[f"{c}_supply" for c in hour_cols]].to_numpy().sum()
        u = sub[used_cols].to_numpy().sum()
        seasonal_rows.append(
            {
                "Season": season,
                "Days": len(sub),
                "Total Demand": d,
                "Total PV Supply (matched)": s,
                "Used Solar": u,
                "Spillage": s - u,
                "Solar Share (%)": (u / d) * 100 if d > 0 else 0.0,
                "Utilisation (%)": (u / s) * 100 if s > 0 else 0.0,
            }
        )

    return annual_summary, pd.DataFrame(seasonal_rows), merged


def save_metrics_workbook(demand_hourly_path: str, supply_hourly_path: str, output_xlsx: str):
    demand_df = read_table(demand_hourly_path)
    supply_df = read_table(supply_hourly_path)
    annual, seasonal, detail = compute_metrics_match_by_monthday_latest(demand_df, supply_df)

    with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
        annual.to_excel(writer, sheet_name="Annual Summary", index=False)
        seasonal.to_excel(writer, sheet_name="Seasonal Summary", index=False)
        detail.to_excel(writer, sheet_name="Matched Detail", index=False)


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Demand + PVGIS Combined GUI")
        self.geometry("980x620")

        self.demand_hh = tk.StringVar()
        self.pvgis_csv = tk.StringVar()
        self.out_dir = tk.StringVar()

        self._build_ui()

    def _browse_file(self, var: tk.StringVar, types):
        path = filedialog.askopenfilename(filetypes=types)
        if path:
            var.set(path)

    def _browse_dir(self):
        path = filedialog.askdirectory()
        if path:
            self.out_dir.set(path)

    def _log(self, msg: str):
        self.log_text.configure(state="normal")
        self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")
        self.update_idletasks()

    def _build_ui(self):
        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Half-hour demand file (.xlsx/.csv)").grid(row=0, column=0, sticky="w")
        ttk.Entry(frm, textvariable=self.demand_hh, width=95).grid(row=1, column=0, sticky="ew", padx=(0, 6))
        ttk.Button(
            frm,
            text="Browse",
            command=lambda: self._browse_file(self.demand_hh, [("Data", "*.xlsx *.xls *.csv")]),
        ).grid(row=1, column=1)

        ttk.Label(frm, text="PVGIS file (.csv)").grid(row=2, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(frm, textvariable=self.pvgis_csv, width=95).grid(row=3, column=0, sticky="ew", padx=(0, 6))
        ttk.Button(
            frm,
            text="Browse",
            command=lambda: self._browse_file(self.pvgis_csv, [("CSV", "*.csv")]),
        ).grid(row=3, column=1)

        ttk.Label(frm, text="Output folder").grid(row=4, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(frm, textvariable=self.out_dir, width=95).grid(row=5, column=0, sticky="ew", padx=(0, 6))
        ttk.Button(frm, text="Browse", command=self._browse_dir).grid(row=5, column=1)

        button_row = ttk.Frame(frm)
        button_row.grid(row=6, column=0, columnspan=2, sticky="w", pady=14)
        ttk.Button(button_row, text="Run all", command=self.run_all).pack(side="left", padx=(0, 8))
        ttk.Button(button_row, text="Demand only (HH → Hourly)", command=self.run_demand_only).pack(
            side="left", padx=(0, 8)
        )
        ttk.Button(button_row, text="PVGIS only", command=self.run_pvgis_only).pack(side="left", padx=(0, 8))

        self.log_text = tk.Text(frm, height=22, state="disabled")
        self.log_text.grid(row=7, column=0, columnspan=2, sticky="nsew")

        frm.grid_columnconfigure(0, weight=1)
        frm.grid_rowconfigure(7, weight=1)

    def _validate_base(self):
        if not self.out_dir.get().strip():
            raise ValueError("Please choose an output folder.")

    def run_demand_only(self):
        try:
            self._validate_base()
            if not self.demand_hh.get().strip():
                raise ValueError("Please choose half-hour demand file.")
            out_path = str(Path(self.out_dir.get()) / "demand_hourly_wide.xlsx")
            write_table(hh_wide_to_hourly_wide_no_day(self.demand_hh.get()), out_path)
            self._log(f"✅ Saved demand hourly file: {out_path}")
        except Exception as exc:
            self._log(f"❌ {exc}")
            messagebox.showerror("Error", str(exc))

    def run_pvgis_only(self):
        try:
            self._validate_base()
            if not self.pvgis_csv.get().strip():
                raise ValueError("Please choose PVGIS CSV file.")
            out_path = str(Path(self.out_dir.get()) / "pvgis_supply_hourly_wide.xlsx")
            write_table(clean_pvgis_to_wide_hourly(self.pvgis_csv.get()), out_path)
            self._log(f"✅ Saved PVGIS hourly file: {out_path}")
        except Exception as exc:
            self._log(f"❌ {exc}")
            messagebox.showerror("Error", str(exc))

    def run_all(self):
        try:
            self._validate_base()
            if not self.demand_hh.get().strip() or not self.pvgis_csv.get().strip():
                raise ValueError("Please pick both demand and PVGIS inputs.")

            outdir = Path(self.out_dir.get())
            demand_hourly = outdir / "demand_hourly_wide.xlsx"
            supply_hourly = outdir / "pvgis_supply_hourly_wide.xlsx"
            profile_plot = outdir / "avg_demand_supply_usedsolar_24h.png"
            profile_table = outdir / "avg_demand_supply_usedsolar_24h.xlsx"
            metrics_book = outdir / "solar_metrics_summary.xlsx"

            self._log("Step 1/4: Converting demand half-hour → hourly...")
            write_table(hh_wide_to_hourly_wide_no_day(self.demand_hh.get()), str(demand_hourly))
            self._log(f"✅ {demand_hourly}")

            self._log("Step 2/4: Cleaning PVGIS and converting to hourly wide...")
            write_table(clean_pvgis_to_wide_hourly(self.pvgis_csv.get()), str(supply_hourly))
            self._log(f"✅ {supply_hourly}")

            self._log("Step 3/4: Building average demand/supply profile + plot...")
            build_average_profile_and_plot(str(demand_hourly), str(supply_hourly), str(profile_plot), str(profile_table))
            self._log(f"✅ {profile_table}")
            self._log(f"✅ {profile_plot}")

            self._log("Step 4/4: Computing annual + seasonal solar metrics...")
            save_metrics_workbook(str(demand_hourly), str(supply_hourly), str(metrics_book))
            self._log(f"✅ {metrics_book}")

            messagebox.showinfo("Done", "Pipeline finished successfully.")
        except Exception as exc:
            self._log(f"❌ {exc}")
            messagebox.showerror("Error", str(exc))


if __name__ == "__main__":
    app = App()
    app.mainloop()
