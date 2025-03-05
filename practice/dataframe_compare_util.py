import dask.dataframe as dd

import dask.dataframe as dd

def compare_dask_dataframes(left_file, right_file, npartitions=100):
    # Load large CSV files into Dask DataFrames with optimized partitions
    left_df = dd.read_csv(left_file, blocksize="64MB").repartition(npartitions=npartitions)
    right_df = dd.read_csv(right_file, blocksize="64MB").repartition(npartitions=npartitions)

    # Get all unique columns from both dataframes
    all_columns = set(left_df.columns).union(set(right_df.columns))

    # List to store comparison results
    results = []

    # Compare columns in parallel using Dask's lazy execution
    for column in all_columns:
        exist_in_left = column in left_df.columns
        exist_in_right = column in right_df.columns

        if exist_in_left and exist_in_right:
            # Compare columns directly using Dask's vectorized operations
            left_vals = left_df[column].fillna("NaN").astype(str)
            right_vals = right_df[column].fillna("NaN").astype(str)

            # Align both columns by their indices using Dask's efficient concat
            merged_df = dd.concat([left_vals.rename("left_val"), right_vals.rename("right_val")], axis=1)

            # Perform comparison and add metadata in parallel
            comparison = merged_df.map_partitions(lambda df: df.assign(
                column_name=column,
                exist_in_left=True,
                exist_in_right=True,
                result=(df["left_val"] == df["right_val"]).map({True: "Pass", False: "Fail"})
            ))

            results.append(comparison)
        else:
            # Handle columns that exist only in one DataFrame
            if exist_in_left:
                left_only = left_df[[column]].fillna("NaN").astype(str).rename(columns={column: "left_val"})
                left_only = left_only.assign(
                    right_val="NaN",
                    column_name=column,
                    exist_in_left=True,
                    exist_in_right=False,
                    result="Fail"
                )
                results.append(left_only)
            if exist_in_right:
                right_only = right_df[[column]].fillna("NaN").astype(str).rename(columns={column: "right_val"})
                right_only = right_only.assign(
                    left_val="NaN",
                    column_name=column,
                    exist_in_left=False,
                    exist_in_right=True,
                    result="Fail"
                )
                results.append(right_only)

    # Concatenate all results and trigger computation at the end for efficiency
    final_result = dd.concat(results).persist()
    return final_result.compute()

# Sample usage

result_df = compare_dask_dataframes('left_file.csv', 'right_file.csv')
print(result_df)
