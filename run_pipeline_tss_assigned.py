
import argparse, os, re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

HALF_HOURS = [f"{h}:{m:02d}" for h in range(0,24) for m in (30,0)][1:]  # 0:30 .. 23:30, then add 24:00 later
HALF_HOURS = ["0:30"] + [f"{h}:00" for h in range(1,24) for _ in (0,) ]  # will rebuild properly below

def build_bins():
    labels=[]
    # 00:00-00:30 is reported as 0:30, then 1:00 ... 23:30, 24:00
    t = timedelta(minutes=30)
    cur = timedelta(minutes=0)
    for i in range(48):
        cur += t
        hh = int(cur.total_seconds()//3600)
        mm = int((cur.total_seconds()%3600)//60)
        if hh==24 and mm==0:
            labels.append("24:00:00")
        else:
            labels.append(f"{hh}:{mm:02d}")
    return labels

BIN_LABELS = build_bins()

def safe_name(s: str) -> str:
    s = str(s)
    s = re.sub(r"[^\w\-]+", "_", s.strip())
    s = re.sub(r"_+", "_", s)
    return s.strip("_") or "UNKNOWN"

def load_or_make_energy_params(path: str, train_types: list[str]):
    if path and os.path.exists(path):
        df = pd.read_csv(path)
        required = {"train_type","kwh_per_km_per_car","aux_kw_per_car"}
        missing = sorted(required - set(df.columns))
        if missing:
            raise ValueError(f"energy_params.csv missing columns: {missing}")
        return df
    # default: conservative placeholders (user should calibrate)
    # kwh_per_km_per_car here is a coarse net traction+loss proxy.
    rows=[]
    for tt in sorted(set(train_types)):
        rows.append({
            "train_type": tt,
            "kwh_per_km_per_car": 2.0,   # placeholder
            "aux_kw_per_car": 10.0,      # placeholder
            "drive_eff": 0.9,
            "regen_eff": 0.0,
            "line_losses_pct": 0.0
        })
    df = pd.DataFrame(rows)
    if path:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=False)
        print(f"[info] wrote default energy_params to {path} (please calibrate values).")
    return df

def parse_date_time(date_str: str, time_str: str) -> datetime:
    # timetable date is dd/mm/yyyy
    d = datetime.strptime(date_str.strip(), "%d/%m/%Y").date()
    t = datetime.strptime(time_str.strip(), "%H:%M").time()
    return datetime.combine(d, t)

def allocate_interval_kwh_to_bins(start: datetime, end: datetime, kwh: float, bin_edges: list[datetime]):
    """Allocate kWh uniformly over [start,end) into half-hour bins defined by bin_edges.
    Returns array len 48."""
    out = np.zeros(48, dtype=float)
    if kwh == 0 or end <= start:
        return out
    total_sec = (end - start).total_seconds()
    rate = kwh / total_sec  # kWh per second
    for i in range(48):
        b0 = bin_edges[i]
        b1 = bin_edges[i+1]
        a0 = max(start, b0)
        a1 = min(end, b1)
        if a1 > a0:
            out[i] += rate * (a1 - a0).total_seconds()
    return out

def make_day_bin_edges(day: datetime) -> list[datetime]:
    d0 = datetime(day.year, day.month, day.day, 0, 0, 0)
    return [d0 + timedelta(minutes=30*i) for i in range(49)]

def expand_services(tt: pd.DataFrame, route: pd.DataFrame, stations: pd.DataFrame, energy_params: pd.DataFrame,
                    split_cross_tss: float = 0.5, regen_eff_default: float = 0.0):
    # maps
    st = stations.copy()
    st["Station_norm"] = st["Station"].astype(str).str.strip()
    tss_map = dict(zip(st["Station_norm"], st["TSS"].astype(str).str.strip()))
    # energy params lookup
    ep = energy_params.set_index("train_type")

    records=[]
    for idx,row in tt.iterrows():
        rv = str(row["route_variant"]).strip()
        date = str(row["date"]).strip()
        dep_time = str(row["dep_time"]).strip()
        train_type = str(row["train_type"]).strip()
        cars = int(row["cars"])
        start_dt = parse_date_time(date, dep_time)

        segs = route[route["route_variant"].astype(str).str.strip() == rv].sort_values("seq")
        if segs.empty:
            continue

        # Build timeline: depart from first from_station
        cur_dt = start_dt

        for _,seg in segs.iterrows():
            from_st = str(seg["from_station"]).strip()
            to_st = str(seg["to_station"]).strip()
            dist_km = float(seg["distance"]) * 1.609344
            run_min = float(seg["run_min"])
            dwell_min = float(seg.get("dwell_time", 0.0))

            # Determine TSSs (must exist)
            from_tss = tss_map.get(from_st)
            to_tss = tss_map.get(to_st)
            if from_tss is None or to_tss is None:
                # keep record with None for debugging
                records.append({
                    "service_row": idx, "route_variant": rv, "from_station": from_st, "to_station": to_st,
                    "missing_tss": True, "from_tss": from_tss, "to_tss": to_tss
                })
                # still advance time so later segments align
                cur_dt = cur_dt + timedelta(minutes=run_min + dwell_min)
                continue

            # Energy model (simple, calibratable):
            params = ep.loc[train_type] if train_type in ep.index else None
            kwh_per_km_per_car = float(params["kwh_per_km_per_car"]) if params is not None else 2.0
            aux_kw_per_car = float(params.get("aux_kw_per_car", 10.0)) if params is not None else 10.0
            line_losses_pct = float(params.get("line_losses_pct", 0.0)) if params is not None else 0.0

            run_kwh = dist_km * kwh_per_km_per_car * cars
            # optional regen if gradient provided (negative grade -> regen)
            if "gradient_percent" in seg.index:
                g = pd.to_numeric(seg["gradient_percent"], errors="coerce")
                if pd.notna(g) and g < 0:
                    # crude: regen saves a fraction of run energy on downhill
                    run_kwh = run_kwh * (1.0 - max(0.0, min(0.9, regen_eff_default)))

            # aux during run + dwell
            aux_kwh_run = aux_kw_per_car * cars * (run_min/60.0)
            aux_kwh_dwell = aux_kw_per_car * cars * (dwell_min/60.0)

            run_kwh_net = (run_kwh + aux_kwh_run) * (1.0 + line_losses_pct)

            # Time windows
            run_start = cur_dt
            run_end = cur_dt + timedelta(minutes=run_min)
            dwell_start = run_end
            dwell_end = run_end + timedelta(minutes=dwell_min)
            cur_dt = dwell_end

            # Split run energy between TSS if crossing boundary
            if from_tss == to_tss:
                records.append({
                    "date": date, "route_variant": rv, "train_type": train_type, "cars": cars,
                    "tss": from_tss, "kind": "run", "start": run_start, "end": run_end, "kwh": run_kwh_net
                })
            else:
                a = max(0.0, min(1.0, split_cross_tss))
                records.append({
                    "date": date, "route_variant": rv, "train_type": train_type, "cars": cars,
                    "tss": from_tss, "kind": "run", "start": run_start, "end": run_end, "kwh": run_kwh_net * a
                })
                records.append({
                    "date": date, "route_variant": rv, "train_type": train_type, "cars": cars,
                    "tss": to_tss, "kind": "run", "start": run_start, "end": run_end, "kwh": run_kwh_net * (1.0 - a)
                })

            # Dwell energy charged to TO station TSS
            if aux_kwh_dwell > 0:
                dwell_kwh_net = aux_kwh_dwell * (1.0 + line_losses_pct)
                records.append({
                    "date": date, "route_variant": rv, "train_type": train_type, "cars": cars,
                    "tss": to_tss, "kind": "dwell", "start": dwell_start, "end": dwell_end, "kwh": dwell_kwh_net
                })

    df = pd.DataFrame(records)
    return df

def aggregate_half_hour(events: pd.DataFrame):
    """Return dict tss -> daily half-hour dataframe"""
    # keep only normal event rows
    ev = events.copy()
    ev = ev[ev.get("missing_tss", False) != True] if "missing_tss" in ev.columns else ev
    if ev.empty:
        return {}

    ev["date_dt"] = pd.to_datetime(ev["date"], dayfirst=True, errors="coerce")
    out_by_tss = {}

    for tss, group in ev.groupby("tss"):
        rows=[]
        for day, gday in group.groupby(group["date_dt"].dt.date):
            day_dt = datetime(day.year, day.month, day.day)
            edges = make_day_bin_edges(day_dt)
            bins = np.zeros(48, dtype=float)
            for _,r in gday.iterrows():
                bins += allocate_interval_kwh_to_bins(r["start"], r["end"], float(r["kwh"]), edges)
            # build row
            total = bins.sum()
            row = {"Date": day_dt.strftime("%d/%m/%Y"), "Day": day_dt.strftime("%A"), "Total Units": total}
            for label, val in zip(BIN_LABELS, bins):
                row[label] = val
            rows.append(row)
        df = pd.DataFrame(rows).sort_values("Date")
        out_by_tss[tss]=df
    return out_by_tss

def main(args):
    tt = pd.read_csv(args.simple_timetable)
    route = pd.read_csv(args.route_template)
    stations = pd.read_csv(args.stations)
    # validate required columns
    for c in ["date","dep_time","route_variant","train_type","cars"]:
        if c not in tt.columns:
            raise ValueError(f"timetable missing column: {c}")
    for c in ["route_variant","seq","from_station","to_station","distance","run_min"]:
        if c not in route.columns:
            raise ValueError(f"route missing column: {c}")
    for c in ["Station","TSS"]:
        if c not in stations.columns:
            raise ValueError(f"station_points missing column: {c}")

    energy_params = load_or_make_energy_params(args.energy_params, tt["train_type"].astype(str).tolist())

    events = expand_services(
        tt=tt,
        route=route,
        stations=stations,
        energy_params=energy_params,
        split_cross_tss=args.split_cross_tss,
        regen_eff_default=args.regen_eff
    )

    # Optional: write debug for missing TSS
    if "missing_tss" in events.columns:
        miss = events[events["missing_tss"]==True]
        if not miss.empty:
            os.makedirs(args.outdir, exist_ok=True)
            miss.to_csv(os.path.join(args.outdir, "DEBUG_missing_tss_rows.csv"), index=False)
            print(f"[warn] wrote {len(miss)} missing-tss rows to DEBUG_missing_tss_rows.csv")

    out_by_tss = aggregate_half_hour(events)

    os.makedirs(args.outdir, exist_ok=True)
    if not out_by_tss:
        print("[warn] no outputs produced (check route_variant joins and station TSS assignments).")
        return

    for tss, df in out_by_tss.items():
        fname = safe_name(tss) + ".csv"
        df.to_csv(os.path.join(args.outdir, fname), index=False)

    print(f"[ok] wrote {len(out_by_tss)} TSS output files to: {args.outdir}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--simple_timetable", required=True)
    p.add_argument("--route_template", required=True)
    p.add_argument("--stations", required=True)
    p.add_argument("--outdir", required=True)
    p.add_argument("--energy_params", default="inputs/energy_params.csv")
    p.add_argument("--regen_eff", type=float, default=0.25)
    p.add_argument("--split_cross_tss", type=float, default=0.5)
    args = p.parse_args()
    main(args)
