import pandas as pd
from datetime import timedelta

DATA_PATH = "data/"
PRINTS_FILE = DATA_PATH + "prints.json"
TAPS_FILE = DATA_PATH + "taps.json"
PAYS_FILE = DATA_PATH + "pays.csv"

# --- 1. File reading ---
prints = pd.read_json(PRINTS_FILE, lines=True)
taps = pd.read_json(TAPS_FILE, lines=True)
pays = pd.read_csv(PAYS_FILE)

# --- 2. Column processing ---
def extract_value_prop_id(df):
    if "value_prop_id" in df.columns:
        return df
    if "value_prop" in df.columns:
        df["value_prop_id"] = df["value_prop"]
    elif "event_data" in df.columns:
        df["value_prop_id"] = df["event_data"].apply(lambda x: x.get("value_prop") if isinstance(x, dict) else None)
    else:
        raise KeyError("Column value_prop_id, value_prop or event_data not found in DataFrame")
    return df

prints = extract_value_prop_id(prints)
taps = extract_value_prop_id(taps)
pays = extract_value_prop_id(pays)

# Convert date columns to datetime (without creating new column)
prints["day"] = pd.to_datetime(prints["day"])
taps["day"] = pd.to_datetime(taps["day"])
pays["pay_date"] = pd.to_datetime(pays["pay_date"])

# --- 3. Define time windows ---
end_date = prints["day"].max()
start_last_week = end_date - timedelta(weeks=1)
start_3weeks_ago = end_date - timedelta(weeks=3)

# --- 4. Prints from last week ---
recent_prints = prints[(prints["day"] >= start_last_week) & (prints["day"] <= end_date)].copy()

# --- 5. Click flag ---
taps_set = taps.drop_duplicates(subset=["user_id", "value_prop_id", "day"])
recent_prints = recent_prints.merge(
    taps_set[["user_id", "value_prop_id", "day"]].rename(columns={"day": "tap_day"}),
    left_on=["user_id", "value_prop_id", "day"],
    right_on=["user_id", "value_prop_id", "tap_day"],
    how="left"
)
recent_prints["clicked"] = recent_prints["tap_day"].notnull().astype(int)
recent_prints = recent_prints.drop(columns=["tap_day"])

# --- 6. Historical features ---
def add_historical_count(df, source, date_col, window_start, window_end, agg_col, new_col, agg_func="count"):
    mask = (source[date_col] < window_end) & (source[date_col] >= window_start)
    filtered = source[mask]
    if agg_func == "count":
        grouped = filtered.groupby(["user_id", "value_prop_id"]).size().reset_index(name=new_col)
    else:
        grouped = filtered.groupby(["user_id", "value_prop_id"])[agg_col].sum().reset_index().rename(columns={agg_col: new_col})
    return df.merge(grouped, on=["user_id", "value_prop_id"], how="left").fillna({new_col: 0})

# print_count_3w
recent_prints = add_historical_count(
    recent_prints, prints, "day", start_3weeks_ago, start_last_week, "value_prop_id", "print_count_3w", "count"
)
# tap_count_3w
recent_prints = add_historical_count(
    recent_prints, taps, "day", start_3weeks_ago, start_last_week, "value_prop_id", "tap_count_3w", "count"
)
# pay_count_3w
recent_prints = add_historical_count(
    recent_prints, pays, "pay_date", start_3weeks_ago, start_last_week, "value_prop_id", "pay_count_3w", "count"
)
# total_amount_3w
recent_prints = add_historical_count(
    recent_prints, pays, "pay_date", start_3weeks_ago, start_last_week, "total", "total_amount_3w", "sum"
)

# --- 7. Save result ---
final_cols = [
    "user_id", "value_prop_id", "day", "clicked",
    "print_count_3w", "tap_count_3w", "pay_count_3w", "total_amount_3w"
]
recent_prints[final_cols].to_csv("dataset_final_pandas.csv", index=False)

print("Dataset generated successfully with Pandas.") 