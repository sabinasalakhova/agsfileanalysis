Triaxial Lab Test AGS Processor
===============================

Overview
--------
This Streamlit-based application processes AGS3/AGS4 geotechnical lab data files (e.g., .ags, .csv, .txt) and generates:
- Merged AGS group tables across multiple files
- Triaxial test summary tables including computed stress path parameters (s and t)
- Excel workbooks with:
  - All AGS groups (one sheet per group)
  - Triaxial summary and computed s–t values
  - Embedded s′–t and s–t scatter charts

Features
--------
- Multi-file upload and merging of AGS groups
- AGS3 & AGS4 parsing with support for GROUP, HEADING, DATA, and <CONT> rows
- Data cleaning: deduplication, expansion of multi-line cells, and removal of empty rows
- Triaxial summary extraction with key fields: HOLE_ID, SPEC_DEPTH, CELL, DEVF, PWPF
- Computation of stress path parameters:
  - t = q/2 = DEVF/2
  - s_total = CELL + DEVF/2
  - s_effective = (CELL - PWPF) + DEVF/2
- Excel export with:
  - All AGS groups
  - Triaxial summary and s–t values
  - Charts: s′–t and s–t scatter plots
- Interactive UI with data preview, filtering, and optional Plotly chart

Installation
------------
1. Clone the repository and navigate to the folder:
   git clone <your-repo-url>
   cd <repo-folder>

2. Create a virtual environment:
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate

3. Install dependencies:
   pip install -r requirements.txt

Requirements
------------
- Python 3.8+
- pandas
- numpy
- streamlit
- plotly
- xlsxwriter

Usage
-----
To run the Streamlit app:
   streamlit run MAINcode.txt

Workflow:
1. Upload one or more AGS files (.ags, .csv, .txt)
2. Review parsed AGS groups in tabs
3. Download:
   - All groups (Excel)
   - Triaxial summary + s–t values + charts (Excel)
4. (Optional) View interactive s–t plot in the app

Excel Output
------------
- Sheet 1: Triaxial_Summary (raw + s,t columns)
- Sheet 2: s_t_Values (computed s_total, s_effective, s, t)
- Sheet 3: Charts (Excel scatter plots for s′–t and s–t)

Customization
-------------
- Change default stress mode (Effective vs Total) in compute_s_t()
- Modify chart styles in add_st_charts_to_excel()
- Add series per TEST_TYPE or SOURCE_FILE for Excel charts
