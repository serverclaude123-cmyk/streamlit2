import streamlit as st
import paho.mqtt.client as mqtt
import json
import time
import ssl
import queue
from datetime import datetime, timezone, timedelta, date
from supabase import create_client, Client
import pandas as pd

# --- 1. CONFIG ---
B            = st.secrets["BROKER"]
U            = st.secrets["USER"]
P            = st.secrets["PASS"]
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
TABLE        = "electrical_log"
WIB          = timezone(timedelta(hours=7))

st.set_page_config(page_title="Industrial Monitor", layout="wide")

# --- 2. SUPABASE CLIENT ---
@st.cache_resource
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

supabase = get_supabase()

# --- 3. SESSION STATE ---
if "data"      not in st.session_state: st.session_state.data = {}
if "status"    not in st.session_state: st.session_state.status = "Connecting..."
if "msg_queue" not in st.session_state: st.session_state.msg_queue = queue.Queue()

# --- 4. KEY CLEANER ---
def clean_key(k: str) -> str:
    return k.replace("/", "_").replace(" ", "_")

def clean_data(data: dict) -> dict:
    return {clean_key(k): v for k, v in data.items()}

# --- 5. LABEL + UNIT MAPPING ---
LABEL_MAP = {
    "A_AC":     ("Current AC",   "A"),
    "A_EV":     ("Current EV",   "A"),
    "W_AC":     ("Power AC",     "W"),
    "W_EV":     ("Power EV",     "W"),
    "KWH_AC":   ("Energy AC",    "kWh"),
    "KWH_EV":   ("Energy EV",    "kWh"),
    "T_MCB_AC": ("Temp MCB AC",  "°C"),
    "T_MCB_EV": ("Temp MCB EV",  "°C"),
    # legacy keys
    "voltage":  ("Voltage",      "V"),
    "power":    ("Power",        "W"),
    "frequency":("Frequency",    "Hz"),
    "pf":       ("Power Factor", ""),
    "energy":   ("Energy",       "kWh"),
}

def get_label(key: str):
    if key in LABEL_MAP:
        label, unit = LABEL_MAP[key]
    else:
        label = key.replace("_", " ").title()
        unit  = ""
    return label, unit

# --- 6. MQTT CALLBACKS ---
def on_connect(client, userdata, flags, rc, props=None):
    if rc == 0:
        st.session_state.status = "✅ CONNECTED"
        client.subscribe("hive/b")
    else:
        st.session_state.status = f"❌ REFUSED (RC: {rc})"

def on_message(client, userdata, msg):
    try:
        raw = msg.payload.decode().strip()
        if raw.startswith('"') and raw.endswith('"'):
            raw = raw[1:-1].replace('\\"', '"')
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            userdata["queue"].put(clean_data(parsed))
        else:
            userdata["queue"].put({"value": str(parsed)})
    except Exception as e:
        userdata["queue"].put({"Raw": msg.payload.decode(), "Error": str(e)})

# --- 7. MQTT CONNECTION ---
if "mqtt_client" not in st.session_state:
    try:
        msg_q = st.session_state.msg_queue
        c = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv311,
            userdata={"queue": msg_q}
        )
        c.username_pw_set(U, P)
        c.tls_set_context(ssl.create_default_context())
        c.on_connect = on_connect
        c.on_message = on_message
        c.connect(B, 8883, keepalive=60)
        c.loop_start()
        st.session_state.mqtt_client = c
    except Exception as e:
        st.session_state.status = f"⚠️ Setup Error: {e}"

# --- 8. DRAIN QUEUE ---
try:
    while True:
        new_data = st.session_state.msg_queue.get_nowait()
        st.session_state.data.update(new_data)
except queue.Empty:
    pass

# --- 9. HELPER: flatten JSONB rows ---
def rows_to_df(rows: list) -> pd.DataFrame:
    records = []
    for row in rows:
        flat = {"timestamp": row["timestamp"]}
        flat.update({clean_key(k): v for k, v in row.get("data", {}).items()})
        records.append(flat)
    df = pd.DataFrame(records)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)
        df["timestamp"] = df["timestamp"].dt.tz_convert(WIB)
    return df

# --- 10. HELPER: KWh delta calculation ---
def compute_kwh_delta(df: pd.DataFrame, col: str, freq: str) -> pd.DataFrame:
    """
    For a cumulative KWh column, compute energy consumed per period.
    freq: 'D' for daily, 'ME' for monthly-end
    Strategy: last reading of period minus first reading of period.
    """
    if col not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=[col])
    df = df.sort_values("timestamp")

    if freq == "D":
        df["period"] = df["timestamp"].dt.date
    else:  # monthly
        df["period"] = df["timestamp"].dt.to_period("M").dt.to_timestamp()

    grouped = df.groupby("period")[col].agg(["first", "last"])
    grouped["kwh_used"] = (grouped["last"] - grouped["first"]).clip(lower=0)
    grouped = grouped.reset_index()
    return grouped[["period", "kwh_used"]]

def fetch_kwh_rows(from_dt: datetime, to_dt: datetime) -> pd.DataFrame:
    """Fetch rows from Supabase for a given WIB datetime range."""
    result = (
        supabase.table(TABLE)
        .select("timestamp, data")
        .gte("timestamp", from_dt.isoformat())
        .lte("timestamp", to_dt.isoformat())
        .order("timestamp", desc=False)
        .limit(50000)
        .execute()
    )
    return rows_to_df(result.data)

# --- 11. UI ---
st.title("🏭 Factory Monitor")
st.subheader(f"System Status: {st.session_state.status}")

tab_live, tab_log, tab_chart, tab_kwh_month, tab_kwh_year = st.tabs([
    "📡 Live Monitor",
    "📋 Data Log",
    "📈 Trend Chart",
    "📅 KWh Monthly View",
    "📆 KWh Yearly View",
])

# ── TAB 1: LIVE ──────────────────────────────────────────────────────────────
with tab_live:
    if st.session_state.data:
        items = [(k, v) for k, v in st.session_state.data.items()
                 if k not in ("timestamp", "Error", "Raw")]
        cols = st.columns(min(len(items), 4))
        for i, (k, v) in enumerate(items):
            col   = cols[i % len(cols)]
            label, unit = get_label(k)
            try:
                col.metric(label=label, value=f"{float(v):.2f} {unit}".strip())
            except (ValueError, TypeError):
                col.metric(label=label, value=str(v))
    else:
        st.info("⏳ Waiting for data from Node-RED...")

    st.caption(f"Last updated: {datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S')} WIB")

# ── TAB 2: DATA LOG ──────────────────────────────────────────────────────────
with tab_log:
    st.markdown("### 📋 Logged Records")

    col_from, col_to, col_fetch = st.columns([2, 2, 1])
    date_from = col_from.date_input("From", value=datetime.now(WIB).date())
    date_to   = col_to.date_input("To",   value=datetime.now(WIB).date())

    if col_fetch.button("🔍 Load", use_container_width=True):
        try:
            result = (
                supabase.table(TABLE)
                .select("timestamp, data")
                .gte("timestamp", f"{date_from}T00:00:00+07:00")
                .lte("timestamp", f"{date_to}T23:59:59+07:00")
                .order("timestamp", desc=True)
                .limit(5000)
                .execute()
            )
            df = rows_to_df(result.data)
            df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
            df = df.rename(columns={k: f"{v[0]} ({v[1]})" if v[1] else v[0]
                                    for k, v in LABEL_MAP.items()})
            st.session_state.log_df = df
        except Exception as e:
            st.error(f"Query failed: {e}")

    if "log_df" in st.session_state and not st.session_state.log_df.empty:
        df = st.session_state.log_df
        col_info, col_dl = st.columns([3, 1])
        col_info.markdown(f"**{len(df)} records** found")
        col_dl.download_button(
            label="⬇️ Download CSV",
            data=df.to_csv(index=False).encode(),
            file_name=f"electrical_log_{date_from}_{date_to}.csv",
            mime="text/csv"
        )
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("Select a date range and click Load.")

# ── TAB 3: TREND CHART ───────────────────────────────────────────────────────
with tab_chart:
    st.markdown("### 📈 Parameter Trend")

    col_p, col_range = st.columns([2, 2])
    param_options = [k for k in st.session_state.data.keys()
                     if k not in ("timestamp", "id", "Error", "Raw")]
    if not param_options:
        param_options = list(LABEL_MAP.keys())

    param_labels = {k: get_label(k)[0] for k in param_options}
    selected_label = col_p.selectbox("Parameter", list(param_labels.values()))
    selected_param = next((k for k, v in param_labels.items() if v == selected_label),
                          param_options[0])

    time_range = col_range.selectbox("Range", [
        "Last 1 hour", "Last 6 hours", "Last 24 hours", "Last 7 days", "Last 30 days"
    ])
    range_map = {
        "Last 1 hour":   timedelta(hours=1),
        "Last 6 hours":  timedelta(hours=6),
        "Last 24 hours": timedelta(hours=24),
        "Last 7 days":   timedelta(days=7),
        "Last 30 days":  timedelta(days=30),
    }
    from_time = (datetime.now(WIB) - range_map[time_range]).isoformat()

    if st.button("📊 Load Chart"):
        try:
            result = (
                supabase.table(TABLE)
                .select("timestamp, data")
                .gte("timestamp", from_time)
                .order("timestamp", desc=False)
                .limit(10000)
                .execute()
            )
            chart_df = rows_to_df(result.data)
            if not chart_df.empty and selected_param in chart_df.columns:
                chart_df = chart_df.set_index("timestamp")
                chart_df[selected_param] = pd.to_numeric(
                    chart_df[selected_param], errors="coerce"
                )
                label, unit = get_label(selected_param)
                st.markdown(f"**{label}** {'(' + unit + ')' if unit else ''}")
                st.line_chart(chart_df[[selected_param]])
            else:
                st.warning(f"No data found for '{selected_label}' in this range.")
        except Exception as e:
            st.error(f"Chart query failed: {e}")

# ── TAB 4: KWh MONTHLY VIEW (daily breakdown) ─────────────────────────────
with tab_kwh_month:
    st.markdown("### 📅 KWh Daily Breakdown — Select a Month")

    now_wib = datetime.now(WIB)

    col_y, col_m, col_src, col_btn = st.columns([1, 1, 2, 1])
    year_sel  = col_y.number_input("Year",  min_value=2020, max_value=now_wib.year,
                                   value=now_wib.year, step=1)
    month_sel = col_m.number_input("Month", min_value=1,    max_value=12,
                                   value=now_wib.month, step=1)
    src_sel   = col_src.selectbox("Meter", ["AC (KWH_AC)", "EV (KWH_EV)", "Both"],
                                  key="src_month")

    if col_btn.button("📅 Load Month", use_container_width=True):
        try:
            # Build WIB start/end for selected month
            start_dt = datetime(int(year_sel), int(month_sel), 1, 0, 0, 0, tzinfo=WIB)
            if int(month_sel) == 12:
                end_dt = datetime(int(year_sel) + 1, 1, 1, 0, 0, 0, tzinfo=WIB) - timedelta(seconds=1)
            else:
                end_dt = datetime(int(year_sel), int(month_sel) + 1, 1, 0, 0, 0, tzinfo=WIB) - timedelta(seconds=1)

            with st.spinner("Fetching data..."):
                df_raw = fetch_kwh_rows(start_dt, end_dt)

            if df_raw.empty:
                st.warning("No data found for this month.")
            else:
                results = {}
                cols_wanted = []
                if src_sel in ("AC (KWH_AC)", "Both"):
                    cols_wanted.append(("KWH_AC", "Energy AC (kWh)"))
                if src_sel in ("EV (KWH_EV)", "Both"):
                    cols_wanted.append(("KWH_EV", "Energy EV (kWh)"))

                for col_key, col_label in cols_wanted:
                    delta = compute_kwh_delta(df_raw, col_key, freq="D")
                    if not delta.empty:
                        delta = delta.rename(columns={"kwh_used": col_label,
                                                      "period": "Date"})
                        delta["Date"] = delta["Date"].astype(str)
                        results[col_label] = delta.set_index("Date")

                if results:
                    # Merge all meter columns into one dataframe
                    merged = pd.concat(results.values(), axis=1)

                    # Summary metrics
                    metric_cols = st.columns(len(results))
                    for i, (lbl, delta_df) in enumerate(results.items()):
                        total = delta_df[lbl].sum()
                        metric_cols[i].metric(label=f"Total {lbl}", value=f"{total:.2f} kWh")

                    st.bar_chart(merged)

                    # Table
                    with st.expander("📋 Daily Detail Table"):
                        merged_display = merged.reset_index()
                        st.dataframe(merged_display.style.format(
                            {c: "{:.2f}" for c in merged.columns}
                        ), use_container_width=True, hide_index=True)

                    # Download
                    csv_data = merged.reset_index().to_csv(index=False).encode()
                    st.download_button(
                        "⬇️ Download Daily KWh CSV",
                        data=csv_data,
                        file_name=f"kwh_daily_{year_sel}_{month_sel:02d}.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("KWH columns (KWH_AC / KWH_EV) not found in logged data. "
                               "Make sure your Node-RED flow is sending those topics.")
        except Exception as e:
            st.error(f"Error: {e}")

    st.caption("💡 Each bar = energy consumed that day (last reading − first reading of the day)")

# ── TAB 5: KWh YEARLY VIEW (monthly breakdown) ───────────────────────────
with tab_kwh_year:
    st.markdown("### 📆 KWh Monthly Breakdown — Select a Year")

    col_y2, col_src2, col_btn2 = st.columns([1, 2, 1])
    year_sel2 = col_y2.number_input("Year",  min_value=2020, max_value=now_wib.year,
                                    value=now_wib.year, step=1, key="year2")
    src_sel2  = col_src2.selectbox("Meter", ["AC (KWH_AC)", "EV (KWH_EV)", "Both"],
                                   key="src_year")

    if col_btn2.button("📆 Load Year", use_container_width=True):
        try:
            start_dt = datetime(int(year_sel2), 1,  1,  0, 0, 0, tzinfo=WIB)
            end_dt   = datetime(int(year_sel2), 12, 31, 23, 59, 59, tzinfo=WIB)

            with st.spinner("Fetching full year data..."):
                df_raw = fetch_kwh_rows(start_dt, end_dt)

            if df_raw.empty:
                st.warning("No data found for this year.")
            else:
                MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun",
                               "Jul","Aug","Sep","Oct","Nov","Dec"]

                results = {}
                cols_wanted = []
                if src_sel2 in ("AC (KWH_AC)", "Both"):
                    cols_wanted.append(("KWH_AC", "Energy AC (kWh)"))
                if src_sel2 in ("EV (KWH_EV)", "Both"):
                    cols_wanted.append(("KWH_EV", "Energy EV (kWh)"))

                for col_key, col_label in cols_wanted:
                    delta = compute_kwh_delta(df_raw, col_key, freq="ME")
                    if not delta.empty:
                        delta["month_num"] = pd.to_datetime(delta["period"]).dt.month
                        delta["Month"]     = delta["month_num"].apply(
                            lambda m: MONTH_NAMES[m - 1])
                        delta = delta.set_index("Month")[["kwh_used"]].rename(
                            columns={"kwh_used": col_label})
                        # Reindex to all 12 months so missing months show as 0
                        delta = delta.reindex(MONTH_NAMES, fill_value=0)
                        results[col_label] = delta

                if results:
                    merged = pd.concat(results.values(), axis=1)

                    # Summary metrics
                    metric_cols = st.columns(len(results) + 1)
                    for i, (lbl, m_df) in enumerate(results.items()):
                        total = m_df[lbl].sum()
                        metric_cols[i].metric(label=f"Total {lbl}", value=f"{total:.2f} kWh")
                    if len(results) > 1:
                        grand_total = sum(m_df[lbl].sum() for lbl, m_df in results.items())
                        metric_cols[-1].metric("Grand Total", f"{grand_total:.2f} kWh")

                    st.bar_chart(merged)

                    # Monthly avg
                    active_months = (merged > 0).any(axis=1).sum()
                    if active_months > 0:
                        st.caption(f"📊 Average per active month: "
                                   f"{merged.sum().sum() / active_months:.2f} kWh")

                    # Table
                    with st.expander("📋 Monthly Detail Table"):
                        st.dataframe(merged.style.format("{:.2f}"),
                                     use_container_width=True)

                    # Download
                    csv_data = merged.reset_index().to_csv(index=False).encode()
                    st.download_button(
                        "⬇️ Download Monthly KWh CSV",
                        data=csv_data,
                        file_name=f"kwh_monthly_{year_sel2}.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("KWH columns not found in logged data for this year.")
        except Exception as e:
            st.error(f"Error: {e}")

    st.caption("💡 Each bar = energy consumed that month (last reading − first reading of the month)")

# --- 12. AUTO-REFRESH ---
time.sleep(2)
st.rerun()
