import dask.dataframe as dd
import pandas as pd
import numpy as np


def compare_dask_dataframes(left_df, right_df, output_file, join_keys=None):
    # Identify all columns across both dataframes excluding join keys
    all_columns = set(left_df.columns).union(set(right_df.columns)) - set(join_keys or [])

    # Track columns that are missing from either dataframe
    missing_in_left = set(right_df.columns) - set(left_df.columns)
    missing_in_right = set(left_df.columns) - set(right_df.columns)

    # Ensure consistent column presence by adding missing columns with NaNs
    for col in all_columns:
        if col not in left_df.columns:
            left_df[col] = np.nan
        if col not in right_df.columns:
            right_df[col] = np.nan

    # Convert all columns to string for uniform comparison
    left_df = left_df.astype(str)
    right_df = right_df.astype(str)

    # Perform join based on provided keys or index
    if join_keys:
        merged_df = left_df.merge(right_df, on=join_keys, how="outer", suffixes=("_left", "_right"), indicator=True)
    else:
        left_df = left_df.reset_index()
        right_df = right_df.reset_index()
        merged_df = left_df.merge(right_df, on="index", how="outer", suffixes=("_left", "_right"), indicator=True)

    def compute_row_differences(row):
        result_rows = []

        for col in all_columns:
            left_value = row.get(f"{col}_left", None)
            right_value = row.get(f"{col}_right", None)

            exist_in_left = pd.notna(left_value)
            exist_in_right = pd.notna(right_value)

            # Variance calculation (only for numeric columns)
            variance = None
            try:
                left_num = float(left_value) if exist_in_left else 0.0
                right_num = float(right_value) if exist_in_right else 0.0
                variance = abs(left_num - right_num)
            except ValueError:
                variance = None  # Ignore variance calculation for non-numeric values

            # Determine result based on updated logic
            if col in missing_in_left or col in missing_in_right:
                result = "Info"
            elif exist_in_left and not exist_in_right:
                result = "Info"
            elif exist_in_right and not exist_in_left:
                result = "Info"
            elif exist_in_left and exist_in_right:
                if left_value == right_value:
                    result = "Pass"
                else:
                    result = "Fail"
            else:
                result = "Fail"

            result_entry = {
                "column_name": col,
                "left_value": left_value,
                "right_value": right_value,
                "exist_in_left": exist_in_left,
                "exist_in_right": exist_in_right,
                "variance": variance,
                "result": result
            }

            # Include join key values in results
            if join_keys:
                for key in join_keys:
                    result_entry[key] = row.get(key, None)

            result_rows.append(result_entry)

        return result_rows

    # Apply row-wise comparison efficiently
    comparison_results = merged_df.map_partitions(
        lambda df: df.apply(compute_row_differences, axis=1).explode().apply(pd.Series))

    # Order results by column_name
    comparison_results = comparison_results.sort_values(by=["column_name"])

    # Save to CSV
    comparison_results.to_csv(output_file, index=False, single_file=True)

    return comparison_results

# Example usage
left_ddf = dd.read_csv('left_file.csv', blocksize='64MB')
right_ddf = dd.read_csv('right_file.csv', blocksize='64MB')
result_df = compare_dask_dataframes(left_ddf, right_ddf, 'comparison_output.csv')
print(result_df.compute())
