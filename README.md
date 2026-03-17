# Electric-Train TSS Pipeline

This project provides:
- `run_pipeline_tss_assigned.py` (CLI pipeline)
- `run_pipeline_tss_gui.py` (desktop GUI)

## 1) Download from Git

```bash
git clone <YOUR_REPO_URL>
cd Electric-Train
```

## 2) Install dependencies

### Windows (PowerShell)

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

If Tkinter is missing on Windows, reinstall Python from python.org and ensure **"tcl/tk and IDLE"** is selected in the installer.

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 3) Run GUI (recommended)

```bash
python run_pipeline_tss_gui.py
```

In the GUI:
1. Browse and select the required CSVs.
2. Pick output directory.
3. Click **Run pipeline**.
4. Results are shown in tabs and exported as per-TSS CSVs.
5. Use **Column summary tools** to pick a TSS and column and compute **sum** or **average**.

## 4) Run CLI (optional)

```bash
python run_pipeline_tss_assigned.py \
  --simple_timetable input2/timetable.csv \
  --route_template input2/route.csv \
  --stations input2/station_points.csv \
  --outdir output_supply \
  --energy_params input2/rolling_stock_energy.csv
```

## 5) Solar + demand combined GUI

A second desktop GUI is included for the four-step solar analysis workflow (HH demand conversion, PVGIS cleanup, average profile plot, seasonal/annual metrics):

```bash
python solar_pipeline_gui.py
```

In this GUI you select:
1. Half-hour demand input file.
2. PVGIS CSV input file.
3. Output folder.

Then click **Run all** to generate:
- `demand_hourly_wide.xlsx`
- `pvgis_supply_hourly_wide.xlsx`
- `avg_demand_supply_usedsolar_24h.xlsx`
- `avg_demand_supply_usedsolar_24h.png`
- `solar_metrics_summary.xlsx`
