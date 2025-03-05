import dask.dataframe as dd


def compare_dask_dataframes(left_file, right_file, blocksize="64MB"):
    # Load CSVs into Dask DataFrames with optimized blocksize
    left_df = dd.read_csv(left_file, blocksize=blocksize)
    right_df = dd.read_csv(right_file, blocksize=blocksize)

    # Ensure both DataFrames have the same index (reset index if needed)
    left_df = left_df.reset_index(drop=True)
    right_df = right_df.reset_index(drop=True)

    # Get all unique columns
    all_columns = set(left_df.columns).union(set(right_df.columns))

    results = []

    for column in all_columns:
        exist_in_left = column in left_df.columns
        exist_in_right = column in right_df.columns

        if exist_in_left and exist_in_right:
            # Merge on index to ensure row-wise comparison
            merged_df = left_df[[column]].merge(right_df[[column]], left_index=True, right_index=True, how="outer",
                                                suffixes=("_left", "_right"))

            # Fill NaN with a placeholder and cast to string
            merged_df = merged_df.fillna("NaN").astype(str)

            # Perform comparison efficiently
            def compare_partition(df):
                df["result"] = df[f"{column}_left"] == df[f"{column}_right"]
                return df.assign(
                    column_name=column,
                    exist_in_left=True,
                    exist_in_right=True,
                    result=df["result"].map({True: "Pass", False: "Fail"})
                )

            results.append(merged_df.map_partitions(compare_partition, meta={
                f"{column}_left": "object", f"{column}_right": "object", "column_name": "object",
                "exist_in_left": "bool", "exist_in_right": "bool", "result": "object"
            }))

        else:
            # Handle columns that exist only in one DataFrame
            if exist_in_left:
                left_only = left_df[[column]].fillna("NaN").astype(str).rename(columns={column: "left_val"})
                left_only["right_val"] = "NaN"
                left_only["column_name"] = column
                left_only["exist_in_left"] = True
                left_only["exist_in_right"] = False
                left_only["result"] = "Fail"
                results.append(left_only)

            if exist_in_right:
                right_only = right_df[[column]].fillna("NaN").astype(str).rename(columns={column: "right_val"})
                right_only["left_val"] = "NaN"
                right_only["column_name"] = column
                right_only["exist_in_left"] = False
                right_only["exist_in_right"] = True
                right_only["result"] = "Fail"
                results.append(right_only)

    # Concatenate results efficiently
    final_result = dd.concat(results, axis=0)

    return final_result.compute()


# Sample usage
result_df = compare_dask_dataframes("left_file.csv", "right_file.csv")
print(result_df)
