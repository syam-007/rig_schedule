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

def process_files(file1, file2, start_date, end_date, selected_party, latest_days, extra_columns):
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

    # Replace N/A or blanks in Gyro Provider with 'MWD' and 'gasway' with 'TASK'
    if "Gyro Provider" in df2.columns:
        df2['Gyro Provider'] = df2['Gyro Provider'].replace(["N/A", "n/a", "NA", "na", None, np.nan, ""], "MWD")
        # Replace 'gasway' with 'TASK' (case insensitive)
        df2['Gyro Provider'] = df2['Gyro Provider'].replace(
            ["gasway", "Gasway", "GASWAY", "gasways", "Gasways", "GASWAYS", "GASWAY "], "TASK")

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
        start_date_raw = row['Earl.start date']
        end_date_raw = row.get('EarliestEndDate', None)

        if isinstance(start_date_raw, str):
            start_date_raw = pd.to_datetime(start_date_raw, errors='coerce')
        if isinstance(end_date_raw, str):
            end_date_raw = pd.to_datetime(end_date_raw, errors='coerce')

        if pd.isna(start_date_raw):
            continue

        well_name = str(row['Well Name']).strip()

        # Special case: RIG MOVE / RIG MAINTENANCE
        if well_name.upper().startswith("RIG MOVE") or well_name.upper().startswith("RIG MAINTENANCE"):
            records.append({
                "Rig": row['Rig_Number'],
                "Well": f"{well_name} ",
                "Job Type": "MAINT",
                "Expected Date": start_date_raw,
                "Latest Expected Date": end_date_raw if not pd.isna(end_date_raw) else start_date_raw,
                **{col: row[col] for col in extra_columns if col in row}
            })
        else:
            for col in callout_cols:
                offset = pd.to_numeric(row[col], errors='coerce')
                if pd.isna(offset):
                    continue
                expected_date = start_date_raw + timedelta(days=int(offset))
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

    # Apply date range filter
    if not result_df.empty and start_date and end_date:
        # Ensure dates are in datetime format
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        # Filter by date range (inclusive of both start and end dates)
        mask = (result_df['Expected Date'] >= start_date) & (result_df['Expected Date'] <= end_date)
        result_df = result_df[mask]

    if result_df.empty:
        return pd.DataFrame()

    # Replace N/A or blanks in Gyro Provider again (post-merge) and 'gasway' with 'TASK'
    if "Gyro Provider" in result_df.columns:
        result_df['Gyro Provider'] = result_df['Gyro Provider'].replace(["N/A", "n/a", "NA", "na", None, np.nan, ""], "MWD")
        # Replace 'gasway' with 'TASK' (case insensitive) - including trailing spaces
        result_df['Gyro Provider'] = result_df['Gyro Provider'].replace(
            ["gasway", "Gasway", "GASWAY", "gasways", "Gasways", "GASWAYS", "GASWAY "], "TASK")

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
        result_df['Latest Expected Date'] = pd.to_datetime(result_df['Latest Expected Date'],
                                                           errors='coerce').dt.strftime('%d-%b-%Y')

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

    # Initialize session state for files
    if 'file1' not in st.session_state:
        st.session_state.file1 = None
    if 'file2' not in st.session_state:
        st.session_state.file2 = None

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📂 Schedule File")
        uploaded_file1 = st.file_uploader("Upload schedule Excel file", type=['xlsx', 'xls'], key="uploader1")
        if uploaded_file1:
            st.session_state.file1 = uploaded_file1
    with col2:
        st.subheader("📂 Rig Details File")
        uploaded_file2 = st.file_uploader("Upload rig details Excel file", type=['xlsx', 'xls'], key="uploader2")
        if uploaded_file2:
            st.session_state.file2 = uploaded_file2

    # Set default date range (next 30 days)
    default_start = datetime.now().date()
    default_end = (datetime.now() + timedelta(days=30)).date()

    # Use session state for dates to persist across reruns
    if 'start_date' not in st.session_state:
        st.session_state.start_date = default_start
    if 'end_date' not in st.session_state:
        st.session_state.end_date = default_end

    parties = ["All"]
    extra_columns = []

    # Get Gyro Provider options from file2 if available
    if st.session_state.file2:
        try:
            # Reset file pointer to beginning
            st.session_state.file2.seek(0)
            df2_temp = read_with_header_detection(st.session_state.file2)
            if 'Gyro Provider' in df2_temp.columns:
                df2_temp['Gyro Provider'] = df2_temp['Gyro Provider'].replace(
                    ["N/A", "n/a", "NA", "na", None, np.nan, ""], "MWD")
                # Replace 'gasway' with 'TASK' (case insensitive) - including trailing spaces
                df2_temp['Gyro Provider'] = df2_temp['Gyro Provider'].replace(
                    ["gasway", "Gasway", "GASWAY", "gasways", "Gasways", "GASWAYS", "GASWAY "], "TASK")
                parties = ["All"] + sorted(df2_temp['Gyro Provider'].dropna().unique().tolist())
        except Exception as e:
            st.error(f"Error reading rig details file: {e}")

    st.subheader("🔧 Filters & Options")
    col3, col4, col5 = st.columns(3)
    with col3:
        # Date range selector
        start_date = st.date_input(
            "📅 Start Date",
            value=st.session_state.start_date,
            help="Select the start date for the date range"
        )
        st.session_state.start_date = start_date
    with col4:
        end_date = st.date_input(
            "📅 End Date",
            value=st.session_state.end_date,
            help="Select the end date for the date range"
        )
        st.session_state.end_date = end_date
    with col5:
        selected_party = st.selectbox("🏢 Select Gyro Provider", parties)

    # Validate date range
    if start_date > end_date:
        st.error("❌ Error: End date must be after start date")
        st.stop()

    latest_days = st.number_input("➕ Add days for Latest Expected Date", min_value=0, max_value=30, value=2)

    # Get extra columns from file1 if available
    if st.session_state.file1:
        try:
            # Reset file pointer to beginning
            st.session_state.file1.seek(0)
            df1_temp = pd.read_excel(st.session_state.file1, nrows=1)
            cols = df1_temp.columns.tolist()
            extra_columns = st.multiselect("📋 Extra columns from Schedule File", cols)
        except Exception as e:
            st.error(f"Error reading schedule file: {e}")

    # Process files when both are uploaded
    if st.session_state.file1 and st.session_state.file2:
        with st.spinner("🔄 Processing..."):
            try:
                # Reset file pointers before processing
                st.session_state.file1.seek(0)
                st.session_state.file2.seek(0)

                result_df = process_files(
                    st.session_state.file1,
                    st.session_state.file2,
                    start_date,
                    end_date,
                    selected_party,
                    latest_days,
                    extra_columns
                )

                if not result_df.empty:
                    st.success(
                        f"✅ Processed {len(result_df)} records for date range {start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}")
                    st.dataframe(result_df, use_container_width=True)

                    # ---------- Export Section ----------
                    csv = result_df.to_csv(index=False)

                    excel_buffer = BytesIO()
                    result_df.to_excel(excel_buffer, index=False, engine='openpyxl')
                    excel_buffer.seek(0)

                    col6, col7 = st.columns(2)
                    with col6:
                        st.download_button(
                            "📥 Download CSV",
                            data=csv,
                            file_name=f"rig_schedule_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.csv",
                            mime="text/csv"
                        )
                    with col7:
                        st.download_button(
                            "📘 Download Excel",
                            data=excel_buffer,
                            file_name=f"rig_schedule_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )

                else:
                    st.warning(
                        f"❌ No matching records found for the date range {start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}.")

            except Exception as e:
                st.error(f"❌ Error processing files: {e}")
                st.info("Please try uploading the files again.")

    elif st.session_state.file1 or st.session_state.file2:
        st.info("📁 Please upload both files to process the data.")


if __name__ == "__main__":
    main()