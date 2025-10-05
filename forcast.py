import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
from io import BytesIO


# ---------- Helpers ----------

def extract_rig_number(work_center):
    """Extract rig number from Work Center column (e.g., SWER101 -> 101, SWERIG99 -> 99)"""
    if pd.isna(work_center):
        return None
    numbers = re.findall(r'\d+', str(work_center))
    if numbers:
        return int(numbers[-1])  # last number in the string
    return None


def read_with_header_detection(file, keywords=["Rig", "Gyro Provider", "Service Type"]):
    """Read Excel and auto-detect correct header row"""
    tmp = pd.read_excel(file, header=None)
    header_row = None
    for i in range(min(10, len(tmp))):  # scan first 10 rows
        row_values = tmp.iloc[i].astype(str).str.lower().tolist()
        if any(k.lower() in " ".join(row_values) for k in keywords):
            header_row = i
            break
    if header_row is None:
        header_row = 0
    df = pd.read_excel(file, header=header_row)
    df.columns = [str(c).strip().replace("\n", " ").replace("#", "No") for c in df.columns]
    return df


# ---------- Core Processing ----------

def process_files(file1, file2, selected_month, selected_year, selected_party, latest_days, extra_columns):
    """Process Excel files and return merged results"""

    # Read first file (schedule)
    df1 = pd.read_excel(file1)
    df1['Rig_Number'] = df1['Work Center'].apply(extract_rig_number)
    df1 = df1.dropna(subset=['Rig_Number'])
    df1['Rig_Number'] = df1['Rig_Number'].astype(int)

    # Read second file (rig details)
    df2 = read_with_header_detection(file2)
    df2['Rig'] = pd.to_numeric(df2['Rig'], errors='coerce')
    df2 = df2.dropna(subset=['Rig'])
    df2['Rig'] = df2['Rig'].astype(int)

    # Replace N/A or blanks in Gyro Provider with 'MWD'
    if "Gyro Provider" in df2.columns:
        df2['Gyro Provider'] = df2['Gyro Provider'].replace(["N/A", "n/a", "NA", "na", None, np.nan, ""], "MWD")

    # Filter by Gyro Provider
    if selected_party != "All" and "Gyro Provider" in df2.columns:
        df2 = df2[df2['Gyro Provider'] == selected_party]

    # Merge schedule + rig details
    merged = pd.merge(
        df1,
        df2,
        left_on='Rig_Number',
        right_on='Rig',
        how='inner'
    )

    if merged.empty:
        return pd.DataFrame()

    # Always include Gyro Provider in the output if available
    if "Gyro Provider" in merged.columns and "Gyro Provider" not in extra_columns:
        extra_columns = ["Gyro Provider"] + extra_columns

    # Expand rows for multiple callout offsets
    callout_cols = [col for col in merged.columns if "Callout" in col and "offset" in col]

    records = []
    for _, row in merged.iterrows():
        start_date = row['Earl.start date']
        end_date = row.get('EarliestEndDate', None)

        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date, errors='coerce')
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date, errors='coerce')

        if pd.isna(start_date):
            continue

        well_name = str(row['Well Name']).strip()

        # Special case: RIG MOVE / RIG MAINTENANCE
        if well_name.upper().startswith("RIG MOVE") or well_name.upper().startswith("RIG MAINTENANCE"):
            records.append({
                "Rig": row['Rig_Number'],
                "Well": f"{well_name} ",
                "Job Type": "MAINT",
                "Expected Date": start_date,
                "Latest Expected Date": end_date if not pd.isna(end_date) else start_date,
                **{col: row[col] for col in extra_columns if col in row}
            })
        else:
            for col in callout_cols:
                offset = pd.to_numeric(row[col], errors='coerce')
                if pd.isna(offset):
                    continue
                expected_date = start_date + timedelta(days=int(offset))
                latest_expected = expected_date + timedelta(days=latest_days)
                records.append({
                    "Rig": row['Rig_Number'],
                    "Well": well_name,
                    "Job Type": row['Service Type'],
                    "Expected Date": expected_date,
                    "Latest Expected Date": latest_expected,
                    **{col: row[col] for col in extra_columns if col in row}
                })

    result_df = pd.DataFrame(records)

    # Apply month/year filter
    if not result_df.empty:
        if selected_month != "All":
            result_df = result_df[result_df['Expected Date'].dt.month == selected_month]
        if selected_year != "All":
            result_df = result_df[result_df['Expected Date'].dt.year == selected_year]

    if result_df.empty:
        return pd.DataFrame()

    # Replace N/A or blanks in Gyro Provider again (post-merge)
    if "Gyro Provider" in result_df.columns:
        result_df['Gyro Provider'] = result_df['Gyro Provider'].replace(["N/A", "n/a", "NA", "na", None, np.nan, ""], "MWD")

    # Separate maintenance rows
    maint_df = result_df[result_df['Job Type'] == "MAINT"].copy()
    normal_df = result_df[result_df['Job Type'] != "MAINT"].copy()
    maint_df["Job Type"] = "Under Maintenance"

    # Combine all
    result_df = pd.concat([normal_df, maint_df], ignore_index=True)
    result_df = result_df.reset_index(drop=True)
    result_df['S.No'] = result_df.index + 1

    # Format dates
    if "Expected Date" in result_df.columns:
        result_df['Expected Date'] = pd.to_datetime(result_df['Expected Date'], errors='coerce').dt.strftime('%d-%b-%Y')
    if "Latest Expected Date" in result_df.columns:
        result_df['Latest Expected Date'] = pd.to_datetime(result_df['Latest Expected Date'], errors='coerce').dt.strftime('%d-%b-%Y')

    result_df.rename(columns={"Expected Date": "Earlier Expected Date"}, inplace=True)

    # Final column order (Gyro Provider added)
    base_cols = ['S.No', 'Rig', 'Well', 'Job Type', 'Gyro Provider', 'Earlier Expected Date', 'Latest Expected Date']

    # Only keep columns that exist
    base_cols = [col for col in base_cols if col in result_df.columns]

    return result_df[base_cols + [col for col in extra_columns if col not in base_cols]]


# ---------- Streamlit UI ----------

def main():
    st.set_page_config(page_title="Rig Schedule Processor", layout="wide")

    st.title("🏗️ Rig Schedule Processor")
    st.markdown("Upload **Schedule File** and **Rig Details File** to generate expected rig schedule.")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📂 Schedule File")
        file1 = st.file_uploader("Upload schedule Excel file", type=['xlsx', 'xls'], key="file1")
    with col2:
        st.subheader("📂 Rig Details File")
        file2 = st.file_uploader("Upload rig details Excel file", type=['xlsx', 'xls'], key="file2")

    months = ["All"] + list(range(1, 13))
    years = ["All"] + list(range(datetime.now().year, datetime.now().year + 5))

    parties = ["All"]
    extra_columns = []
    if file2:
        try:
            df2_temp = read_with_header_detection(file2)
            if 'Gyro Provider' in df2_temp.columns:
                df2_temp['Gyro Provider'] = df2_temp['Gyro Provider'].replace(["N/A", "n/a", "NA", "na", None, np.nan, ""], "MWD")
                parties = ["All"] + sorted(df2_temp['Gyro Provider'].dropna().unique().tolist())
        except:
            pass

    st.subheader("🔧 Filters & Options")
    col3, col4, col5 = st.columns(3)
    with col3:
        selected_month = st.selectbox("📅 Select Month", months)
    with col4:
        selected_year = st.selectbox("📅 Select Year", years)
    with col5:
        selected_party = st.selectbox("🏢 Select Gyro Provider", parties)

    latest_days = st.number_input("➕ Add days for Latest Expected Date", min_value=0, max_value=30, value=2)

    if file1:
        df1_temp = pd.read_excel(file1, nrows=1)
        cols = df1_temp.columns.tolist()
        extra_columns = st.multiselect("📋 Extra columns from Schedule File", cols)

    if file1 and file2:
        with st.spinner("🔄 Processing..."):
            result_df = process_files(file1, file2, selected_month, selected_year, selected_party, latest_days, extra_columns)

        if not result_df.empty:
            st.success(f"✅ Processed {len(result_df)} records")
            st.dataframe(result_df, use_container_width=True)

            # ---------- Export Section ----------
            csv = result_df.to_csv(index=False)

            excel_buffer = BytesIO()
            result_df.to_excel(excel_buffer, index=False, engine='openpyxl')
            excel_buffer.seek(0)

            st.download_button(
                "📥 Download CSV",
                data=csv,
                file_name=f"rig_schedule_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv"
            )

            st.download_button(
                "📘 Download Excel",
                data=excel_buffer,
                file_name=f"rig_schedule_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        else:
            st.warning("❌ No matching records found for the selected filters.")


if __name__ == "__main__":
    main()
