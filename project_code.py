"""
COMP 5700 Final Project
"""

import pandas as pd
from pathlib import Path
import re

# Setup paths
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

# Create output directory
OUTPUT_DIR.mkdir(exist_ok=True)

def save_csv_to_folder(df, base_csv_name, output_dir, max_rows_per_file=25000):
    """
    Save CSV file(s) into a dedicated folder under output.
    If the dataset is large, split into multiple part CSV files.
    Returns list of generated CSV paths.
    """
    folder_name = base_csv_name.replace('.csv', '')
    csv_folder = output_dir / folder_name
    csv_folder.mkdir(exist_ok=True)

    total_rows = len(df)
    csv_paths = []

    if total_rows <= max_rows_per_file:
        csv_path = csv_folder / base_csv_name
        df.to_csv(csv_path, index=False)
        print(f"Saved: {csv_path}")
        csv_paths.append(csv_path)
        return csv_paths

    # Split into parts
    num_parts = (total_rows // max_rows_per_file) + (1 if total_rows % max_rows_per_file else 0)
    for i in range(num_parts):
        start_idx = i * max_rows_per_file
        end_idx = min((i + 1) * max_rows_per_file, total_rows)
        df_part = df.iloc[start_idx:end_idx]
        part_name = base_csv_name.replace('.csv', f'_part{i+1}.csv')
        csv_path = csv_folder / part_name
        df_part.to_csv(csv_path, index=False)
        print(f"Saved part {i+1}/{num_parts}: {csv_path} ({len(df_part):,} rows)")
        csv_paths.append(csv_path)

    return csv_paths

# Data file paths
PULL_REQUEST_FILE = DATA_DIR / "all_pull_request.parquet"
REPOSITORY_FILE = DATA_DIR / "all_repository.parquet"
TASK_TYPE_FILE = DATA_DIR / "pr_task_type.parquet"
COMMIT_DETAILS_FILE = DATA_DIR / "pr_commit_details.parquet"

print("=" * 80)
print("COMP 5700 Final Project: Tasks 1-5")
print("=" * 80)

# Load all parquet files
print("\nLoading data files...")
df_pull_request = pd.read_parquet(PULL_REQUEST_FILE)
df_repository = pd.read_parquet(REPOSITORY_FILE)
df_task_type = pd.read_parquet(TASK_TYPE_FILE)
df_commit_details = pd.read_parquet(COMMIT_DETAILS_FILE)

print(f"  Loaded all_pull_request: {df_pull_request.shape[0]:,} rows")
print(f"  Loaded all_repository: {df_repository.shape[0]:,} rows")
print(f"  Loaded pr_task_type: {df_task_type.shape[0]:,} rows")
print(f"  Loaded pr_commit_details: {df_commit_details.shape[0]:,} rows")

# ============================================================================
# TASK 1: Extract Pull Request Data
# ============================================================================
print("\n" + "=" * 80)
print("TASK 1: Creating Pull Request CSV")
print("=" * 80)

task1_df = df_pull_request[['title', 'id', 'agent', 'body', 'repo_id', 'repo_url']].copy()
task1_df.columns = ['TITLE', 'ID', 'AGENTNAME', 'BODYSTRING', 'REPOID', 'REPOURL']

task1_output = save_csv_to_folder(task1_df, "all_pull_request.csv", OUTPUT_DIR)
print(f"  Rows: {len(task1_df):,}")
print(f"  Columns: {list(task1_df.columns)}")

# ============================================================================
# TASK 2: Extract Repository Data
# ============================================================================
print("\n" + "=" * 80)
print("TASK 2: Creating Repository CSV")
print("=" * 80)

task2_df = df_repository[['id', 'language', 'stars', 'url']].copy()
task2_df.columns = ['REPOID', 'LANG', 'STARS', 'REPOURL']

task2_output = save_csv_to_folder(task2_df, "all_repository.csv", OUTPUT_DIR)
print(f"  Rows: {len(task2_df):,}")
print(f"  Columns: {list(task2_df.columns)}")

# ============================================================================
# TASK 3: Extract PR Task Type Data
# ============================================================================
print("\n" + "=" * 80)
print("TASK 3: Creating PR Task Type CSV")
print("=" * 80)

task3_df = df_task_type[['id', 'title', 'reason', 'type', 'confidence']].copy()
task3_df.columns = ['PRID', 'PRTITLE', 'PRREASON', 'PRTYPE', 'CONFIDENCE']

task3_output = save_csv_to_folder(task3_df, "pr_task_type.csv", OUTPUT_DIR)
print(f"  Rows: {len(task3_df):,}")
print(f"  Columns: {list(task3_df.columns)}")

# ============================================================================
# TASK 4: Extract PR Commit Details with Cleaned Patch
# ============================================================================
print("\n" + "=" * 80)
print("TASK 4: Creating PR Commit Details CSV")
print("=" * 80)

def clean_patch(patch_text):
    """
    Remove special characters from patch to avoid string encoding errors.
    Keep only printable ASCII characters, tabs, newlines, and carriage returns.
    """
    if pd.isna(patch_text):
        return ""
    # Keep only ASCII printable chars, tab, newline, and carriage return
    return re.sub(r'[^\x09\x0A\x0D\x20-\x7E]', '', str(patch_text))

print("  Cleaning patch data to remove special characters...")
df_commit_details['cleaned_patch'] = df_commit_details['patch'].apply(clean_patch)

task4_df = df_commit_details[[
    'pr_id', 'sha', 'message', 'filename', 'status',
    'additions', 'deletions', 'changes', 'cleaned_patch'
]].copy()

task4_df.columns = [
    'PRID', 'PRSHA', 'PRCOMMITMESSAGE', 'PRFILE', 'PRSTATUS',
    'PRADDS', 'PRDELSS', 'PRCHANGECOUNT', 'PRDIFF'
]

task4_output = save_csv_to_folder(task4_df, "pr_commit_details.csv", OUTPUT_DIR)
print(f"  Rows: {len(task4_df):,}")
print(f"  Columns: {list(task4_df.columns)}")

# ============================================================================
# TASK 5: Create Security Summary
# ============================================================================
print("\n" + "=" * 80)
print("TASK 5: Creating Security Summary CSV")
print("=" * 80)

# Security keywords from the requirements
SECURITY_KEYWORDS = [
    'race', 'racy', 'buffer', 'overflow', 'stack', 'integer', 'signedness',
    'underflow', 'improper', 'unauthenticated', 'gain access', 'permission',
    'cross site', 'css', 'xss', 'denial service', 'dos', 'crash', 'deadlock',
    'injection', 'request forgery', 'csrf', 'xsrf', 'forged', 'security',
    'vulnerability', 'vulnerable', 'exploit', 'attack', 'bypass', 'backdoor',
    'threat', 'expose', 'breach', 'violate', 'fatal', 'blacklist', 'overrun',
    'insecure'
]

# Create regex pattern for efficient searching
security_pattern = re.compile(
    '|'.join(re.escape(keyword) for keyword in SECURITY_KEYWORDS),
    re.IGNORECASE
)

def check_security(title, body):
    """
    Check if title or body contains any security-related keywords.
    Returns 1 if security keyword found, 0 otherwise.
    """
    combined_text = f"{title if pd.notna(title) else ''} {body if pd.notna(body) else ''}"
    return 1 if security_pattern.search(combined_text) else 0

# Merge data from Task 1 and Task 3
print("  Merging pull request and task type data...")
task5_df = task1_df[['ID', 'AGENTNAME', 'TITLE', 'BODYSTRING']].merge(
    task3_df[['PRID', 'PRTYPE', 'CONFIDENCE']],
    left_on='ID',
    right_on='PRID',
    how='inner'
)

print("  Checking for security keywords in titles and bodies...")
task5_df['SECURITY'] = task5_df.apply(
    lambda row: check_security(row['TITLE'], row['BODYSTRING']),
    axis=1
)

# Select and rename final columns
task5_df = task5_df[['ID', 'AGENTNAME', 'PRTYPE', 'CONFIDENCE', 'SECURITY']].copy()
task5_df.columns = ['ID', 'AGENT', 'TYPE', 'CONFIDENCE', 'SECURITY']

task5_output = save_csv_to_folder(task5_df, "pr_security_summary.csv", OUTPUT_DIR)
print(f"  Rows: {len(task5_df):,}")
print(f"  Columns: {list(task5_df.columns)}")
print(f"  Security-related PRs (SECURITY=1): {(task5_df['SECURITY'] == 1).sum():,}")
print(f"  Non-security PRs (SECURITY=0): {(task5_df['SECURITY'] == 0).sum():,}")

# ============================================================================
# Final Summary
# ============================================================================
print("\n" + "=" * 80)
print("SUMMARY: All Tasks Completed Successfully")
print("=" * 80)
print(f"\nTask 1: output/all_pull_request/ - {len(task1_df):,} rows")
print(f"Task 2: output/all_repository/ - {len(task2_df):,} rows")
print(f"Task 3: output/pr_task_type/ - {len(task3_df):,} rows")
print(f"Task 4: output/pr_commit_details/ - {len(task4_df):,} rows")
print(f"Task 5: output/pr_security_summary/ - {len(task5_df):,} rows")
print("\nAll output files saved to individual folders in output/ directory")
print("=" * 80)
