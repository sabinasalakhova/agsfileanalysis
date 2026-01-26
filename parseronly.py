import pandas as pd
import streamlit as st
import io

# Advanced "Build Your Own Excel" implementation
with st.sidebar:
    st.header("Build Your Own Excel")

    if combined_groups:  # Ensure there are groups to work with
        # Select groups to include
        selected_groups = st.multiselect(
            "Select groups to include:",
            options=combined_groups.keys(),
            default=list(combined_groups.keys()),
            help="Select groups from the uploaded files."
        )

        # Advanced toggle for concatenation
        concat_option = st.checkbox(
            "Concatenate all selected groups into a single sheet",
            value=False,
            help="Enable this to combine all selected groups into one sheet in the output Excel file."
        )

        # Dynamic column selection for individual groups
        group_column_selections = {}
        for group_name in selected_groups:
            st.subheader(f"Group: {group_name}")
            available_columns = list(combined_groups[group_name].columns)
            
            group_column_selections[group_name] = st.multiselect(
                f"Columns for '{group_name}'",
                options=available_columns,
                default=available_columns,  # Default to all columns
                help=f"Select columns to include in group '{group_name}'."
            )

        # Optional filtering (by HOLE_TYPE or depth ranges)
        st.subheader("Advanced Filtering (Optional)")
        enable_row_filters = st.checkbox(
            "Enable Row Filtering",
            value=False,
            help="Check this to filter rows (e.g., by specific column values)."
        )

        row_filters = {}
        if enable_row_filters:
            for group_name in selected_groups:
                st.subheader(f"Filtering for Group: {group_name}")
                sample_rows = combined_groups[group_name].sample(min(5, len(combined_groups[group_name]))).to_dict(orient="records")

                st.table(sample_rows)  # Show a sample of rows for context (first 5)
                for column in group_column_selections[group_name]:
                    unique_values = combined_groups[group_name][column].dropna().unique().tolist()
                    row_filters[column] = st.multiselect(
                        f"Filter {column} in group '{group_name}'",
                        options=unique_values,
                        default=[],
                        help=f"Select specific values to include for column '{column}' in group '{group_name}'. Leave blank to include all."
                    )

        # Generate the "Build Your Own Excel" file
        if st.button("Generate Custom Excel File"):
            st.info("Processing your custom Excel...")
            
            custom_buffer = io.BytesIO()
            with pd.ExcelWriter(custom_buffer, engine="xlsxwriter") as writer:
                if concat_option:
                    # Concatenate data across groups
                    concatenated_df = pd.DataFrame()
                    for group_name in selected_groups:
                        group_df = combined_groups[group_name]

                        # Filter rows based on row_filters
                        if enable_row_filters:
                            for column, allowed_values in row_filters.items():
                                if allowed_values:
                                    group_df = group_df[group_df[column].isin(allowed_values)]

                        # Select columns
                        valid_columns = group_column_selections[group_name]
                        group_df = group_df[valid_columns]

                        # Add group identifier column
                        group_df['SOURCE_GROUP'] = group_name

                        concatenated_df = pd.concat([concatenated_df, group_df], ignore_index=True)

                    # Save to Excel
                    concatenated_df.to_excel(writer, index=False, sheet_name="Concatenated Groups")
                else:
                    # Separate sheets for each group
                    for group_name in selected_groups:
                        group_df = combined_groups[group_name]

                        # Filter rows and select columns
                        if enable_row_filters:
                            for column, allowed_values in row_filters.items():
                                if allowed_values:
                                    group_df = group_df[group_df[column].isin(allowed_values)]

                        valid_columns = group_column_selections[group_name]
                        group_df = group_df[valid_columns]

                        # Save individual sheet
                        group_df.to_excel(writer, index=False, sheet_name=group_name[:31])
            
            # Allow user to download the file
            st.download_button(
                label="📥 Download Custom Excel File",
                data=custom_buffer.getvalue(),
                file_name="custom_ags_groups.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Custom Excel file generated based on your selections."
            )
    else:
        st.warning("No data groups available for customizing.")
