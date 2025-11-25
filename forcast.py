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


def read_with_header_detection(file,
                               keywords=["Rig", "Gyro Provider", "Service Type", "DD Company", "Cluster", "Team"]):
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


def clean_column_names(df):
    """Clean column names and handle duplicates"""
    # Clean column names
    df.columns = [str(col).strip().replace('\n', ' ').replace('  ', ' ') for col in df.columns]

    # Handle duplicate column names
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols == dup] = [f'{dup}_{i}' if i != 0 else dup for i in range(sum(cols == dup))]

    df.columns = cols
    return df


def find_column_by_pattern(df, patterns):
    """Find column name by matching patterns"""
    for pattern in patterns:
        for col in df.columns:
            if pattern.lower() in str(col).lower():
                return col
    return None


# ---------- Core Processing ----------

def process_files(file1, file2, start_date, end_date, selected_party, latest_days, extra_columns):
    """Process Excel files and return merged results"""

    # Read first file (schedule)
    df1 = pd.read_excel(file1)
    df1 = clean_column_names(df1)
    df1['Rig_Number'] = df1['Work Center'].apply(extract_rig_number)
    df1 = df1.dropna(subset=['Rig_Number'])
    df1['Rig_Number'] = df1['Rig_Number'].astype(int)

    # Read second file (rig details)
    df2 = read_with_header_detection(file2)
    df2 = clean_column_names(df2)

    # Find Rig column using multiple patterns
    rig_col = find_column_by_pattern(df2, ['rig', 'rig no', 'rig number', 'rig#'])
    if rig_col is None:
        # If no rig column found, use the first column that contains numbers
        for col in df2.columns:
            if df2[col].dtype in ['int64', 'float64'] or df2[col].dropna().apply(lambda x: str(x).isdigit()).any():
                rig_col = col
                break

    if rig_col is None:
        st.error("❌ Could not find Rig column in rig details file")
        return pd.DataFrame()

    df2['Rig'] = pd.to_numeric(df2[rig_col], errors='coerce')
    df2 = df2.dropna(subset=['Rig'])
    df2['Rig'] = df2['Rig'].astype(int)

    # Find and clean Gyro Provider column
    gyro_col = find_column_by_pattern(df2, ['gyro', 'provider', 'gyro provider'])
    if gyro_col:
        df2[gyro_col] = df2[gyro_col].replace(
            ["N/A", "n/a", "NA", "na", None, np.nan, ""], "MWD"
        )
        # Rename to standard name for consistency
        df2.rename(columns={gyro_col: 'Gyro Provider'}, inplace=True)

    # Find DD Company column
    dd_company_col = find_column_by_pattern(df2, ['dd company', 'dd', 'company', 'dd co'])
    if dd_company_col and dd_company_col != 'Gyro Provider':  # Ensure it's not the same as gyro provider
        df2.rename(columns={dd_company_col: 'DD Company'}, inplace=True)

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
        st.warning("❌ No matching rigs found between schedule and rig details files")
        return pd.DataFrame()

    # Always include Gyro Provider in the output if available
    if "Gyro Provider" in merged.columns and "Gyro Provider" not in extra_columns:
        extra_columns = ["Gyro Provider"] + extra_columns

    # Always include DD Company if it exists in the data
    if "DD Company" in merged.columns and "DD Company" not in extra_columns:
        extra_columns = ["DD Company"] + extra_columns

    # Expand rows for multiple callout offsets
    callout_cols = [col for col in merged.columns if "callout" in str(col).lower() and "offset" in str(col).lower()]

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

        well_name = str(row['Well Name']).strip() if 'Well Name' in row else "Unknown Well"

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
                    "Job Type": row.get('Service Type', 'Unknown Service'),
                    "Expected Date": expected_date,
                    "Latest Expected Date": latest_expected,
                    **{col: row[col] for col in extra_columns if col in row}
                })

    result_df = pd.DataFrame(records)

    # Apply date range filter
    if not result_df.empty and start_date and end_date:
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        mask = (result_df['Expected Date'] >= start_date) & (result_df['Expected Date'] <= end_date)
        result_df = result_df[mask]

    if result_df.empty:
        return pd.DataFrame()

    # Replace N/A or blanks in Gyro Provider again (post-merge)
    if "Gyro Provider" in result_df.columns:
        result_df['Gyro Provider'] = result_df['Gyro Provider'].replace(
            ["N/A", "n/a", "NA", "na", None, np.nan, ""], "MWD"
        )

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
        result_df['Expected Date'] = pd.to_datetime(
            result_df['Expected Date'], errors='coerce'
        ).dt.strftime('%d-%b-%Y')
    if "Latest Expected Date" in result_df.columns:
        result_df['Latest Expected Date'] = pd.to_datetime(
            result_df['Latest Expected Date'], errors='coerce'
        ).dt.strftime('%d-%b-%Y')

    result_df.rename(columns={"Expected Date": "Earlier Expected Date"}, inplace=True)

    # Final column order
    base_cols = ['S.No', 'Rig', 'Well', 'Job Type',
                 'Gyro Provider', 'Earlier Expected Date', 'Latest Expected Date']

    base_cols = [col for col in base_cols if col in result_df.columns]

    # Remove any duplicate columns that might have been added
    final_columns = base_cols + [col for col in extra_columns if col not in base_cols and col in result_df.columns]
    final_columns = list(dict.fromkeys(final_columns))  # Remove duplicates while preserving order

    return result_df[final_columns]


# ---------- Streamlit UI ----------

def main():
    st.set_page_config(page_title="Rig Schedule Processor", layout="wide")

    st.title("🏗️ Rig Schedule Processor")
    st.markdown("Upload **Schedule File** and **Rig Details File** to generate expected rig schedule.")

    # Initialize session state
    if 'file1' not in st.session_state:
        st.session_state.file1 = None
    if 'file2' not in st.session_state:
        st.session_state.file2 = None
    if 'processed' not in st.session_state:
        st.session_state.processed = False
    if 'result_df' not in st.session_state:
        st.session_state.result_df = None
    if 'extra_columns' not in st.session_state:
        st.session_state.extra_columns = []

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📂 Schedule File")
        uploaded_file1 = st.file_uploader(
            "Upload schedule Excel file", type=['xlsx', 'xls'], key="uploader1"
        )
        # Check if file was removed
        if uploaded_file1 is None and st.session_state.file1 is not None:
            st.session_state.file1 = None
            st.session_state.processed = False
            st.session_state.result_df = None
            st.error("❌ Schedule file has been removed. Please upload both files again.")
        elif uploaded_file1 is not None:
            st.session_state.file1 = uploaded_file1
            st.session_state.processed = False  # Reset processed state when file changes

    with col2:
        st.subheader("📂 Rig Details File")
        uploaded_file2 = st.file_uploader(
            "Upload rig details Excel file", type=['xlsx', 'xls'], key="uploader2"
        )
        # Check if file was removed
        if uploaded_file2 is None and st.session_state.file2 is not None:
            st.session_state.file2 = None
            st.session_state.processed = False
            st.session_state.result_df = None
            st.error("❌ Rig details file has been removed. Please upload both files again.")
        elif uploaded_file2 is not None:
            st.session_state.file2 = uploaded_file2
            st.session_state.processed = False  # Reset processed state when file changes

    # Set default date range (next 30 days)
    default_start = datetime.now().date()
    default_end = (datetime.now() + timedelta(days=30)).date()

    if 'start_date' not in st.session_state:
        st.session_state.start_date = default_start
    if 'end_date' not in st.session_state:
        st.session_state.end_date = default_end

    parties = ["All"]
    extra_columns = []

    # ---------- Gyro Provider options from rig details file ----------
    if st.session_state.file2:
        try:
            st.session_state.file2.seek(0)
            df2_temp = read_with_header_detection(st.session_state.file2)

            # Find Gyro Provider column
            gyro_col = find_column_by_pattern(df2_temp, ['gyro', 'provider', 'gyro provider'])
            if gyro_col:
                df2_temp[gyro_col] = df2_temp[gyro_col].replace(
                    ["N/A", "n/a", "NA", "na", None, np.nan, ""], "MWD"
                )
                parties = ["All"] + sorted(df2_temp[gyro_col].dropna().unique().tolist())
        except Exception as e:
            st.error(f"Error reading rig details file: {e}")

    # ---------- Filters & Options ----------
    st.subheader("🔧 Filters & Options")
    col3, col4, col5 = st.columns(3)
    with col3:
        start_date = st.date_input(
            "📅 Start Date",
            value=st.session_state.start_date,
            help="Select the start date for the date range"
        )
    with col4:
        end_date = st.date_input(
            "📅 End Date",
            value=st.session_state.end_date,
            help="Select the end date for the date range"
        )
    with col5:
        selected_party = st.selectbox("🏢 Select Gyro Provider", parties)

    if start_date > end_date:
        st.error("❌ Error: End date must be after start date")
        st.stop()

    latest_days = st.number_input(
        "➕ Add days for Latest Expected Date", min_value=0, max_value=30, value=2
    )

    # ---------- Column selection logic ----------
    schedule_cols = []
    rig_cols = []
    combined_cols = []

    # Only show column selection if both files are uploaded
    if st.session_state.file1 and st.session_state.file2:
        try:
            # Read one row from schedule for column names
            st.session_state.file1.seek(0)
            df1_temp = pd.read_excel(st.session_state.file1, nrows=1)
            df1_temp = clean_column_names(df1_temp)
            schedule_cols = df1_temp.columns.tolist()

            # Read rig details for column names (with header detection)
            st.session_state.file2.seek(0)
            df2_cols_only = read_with_header_detection(st.session_state.file2)
            df2_cols_only = clean_column_names(df2_cols_only)
            rig_cols = df2_cols_only.columns.tolist()

            # Combined columns from both files
            combined_cols = list(dict.fromkeys(schedule_cols + rig_cols))

            st.markdown("### 📋 Column Selection")

            # Column mode selection
            column_mode = st.radio(
                "Choose which columns to show:",
                ["Custom", "All", "Schedule File only", "Rig Details File only"],
                index=0,
                key="column_mode_selector"
            )

            # Set extra_columns based on mode
            if column_mode == "All":
                extra_columns = combined_cols
            elif column_mode == "Schedule File only":
                extra_columns = schedule_cols
            elif column_mode == "Rig Details File only":
                extra_columns = rig_cols
            else:  # Custom
                # Default selection for custom mode
                default_selection = []
                if "DD Company" in combined_cols:
                    default_selection.append("DD Company")
                if "Cluster" in combined_cols:
                    default_selection.append("Cluster")
                if "Team" in combined_cols:
                    default_selection.append("Team")

                extra_columns = st.multiselect(
                    "📋 Select extra columns to display",
                    combined_cols,
                    default=default_selection,
                    key="custom_columns"
                )

        except Exception as e:
            st.error(f"Error reading columns: {e}")

    # Info messages when only one file is uploaded
    if (st.session_state.file1 and not st.session_state.file2) or (
            not st.session_state.file1 and st.session_state.file2):
        st.info("📁 Please upload both files to proceed.")

    # ---------- Process Button ----------
    process_clicked = False
    if st.session_state.file1 and st.session_state.file2:
        st.markdown("---")
        process_clicked = st.button("🚀 Process Files", type="primary", use_container_width=True)
    elif st.session_state.processed and (not st.session_state.file1 or not st.session_state.file2):
        # Clear results if files are removed
        st.session_state.processed = False
        st.session_state.result_df = None
        st.error("❌ One or both files have been removed. Please upload both files and process again.")

    # ---------- Processing & Output ----------
    if process_clicked and st.session_state.file1 and st.session_state.file2:
        with st.spinner("🔄 Processing files..."):
            try:
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

                st.session_state.result_df = result_df
                st.session_state.processed = True

            except Exception as e:
                st.error(f"❌ Error processing files: {e}")
                st.info("Please check your files and try again.")

    # Display results if processed AND both files are still available
    if (st.session_state.processed and
            st.session_state.result_df is not None and
            st.session_state.file1 and
            st.session_state.file2):

        result_df = st.session_state.result_df

        if not result_df.empty:
            st.success(
                f"✅ Processed {len(result_df)} records for date range "
                f"{start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}"
            )

            st.dataframe(result_df, use_container_width=True)

            # Export functionality
            csv = result_df.to_csv(index=False)

            excel_buffer = BytesIO()
            result_df.to_excel(excel_buffer, index=False, engine='openpyxl')
            excel_buffer.seek(0)

            col6, col7 = st.columns(2)
            with col6:
                st.download_button(
                    "📥 Download CSV",
                    data=csv,
                    file_name=(
                        f"rig_schedule_{start_date.strftime('%Y%m%d')}_"
                        f"to_{end_date.strftime('%Y%m%d')}.csv"
                    ),
                    mime="text/csv"
                )
            with col7:
                st.download_button(
                    "📘 Download Excel",
                    data=excel_buffer,
                    file_name=(
                        f"rig_schedule_{start_date.strftime('%Y%m%d')}_"
                        f"to_{end_date.strftime('%Y%m%d')}.xlsx"
                    ),
                    mime=(
                        "application/vnd.openxmlformats-"
                        "officedocument.spreadsheetml.sheet"
                    )
                )

        else:
            st.warning(
                f"❌ No matching records found for the date range "
                f"{start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}."
            )

    # Show error if files are missing but results were previously shown
    elif st.session_state.processed and (not st.session_state.file1 or not st.session_state.file2):
        st.error("❌ Files have been removed. Please upload both files and process again.")


if __name__ == "__main__":
    main()