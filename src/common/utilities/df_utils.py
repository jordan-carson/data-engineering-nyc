import pandas as pd

# TODO Create cardinality function for multiple dtypes(df, array, series, list)
# High is a lot of distinct values low is a lot of repeated values


def calc_cardinality(iterable, verbose=False):
    """Function to calculate the cardinality of a pandas series or numpy array.
       If a pandas dataframe is passed then all object columns are looped through
       and their cardinality returned.

       Value is between 0 and 100

    Arguments:
        iterable {iterable} -- pandas series, dataframe, or numpy array
    """

    if isinstance(iterable, pd.DataFrame):
        print("<===== Cardinality Score =====>")
        rows = iterable.shape[1]
        df_str = iterable.select_dtypes(include=[object])
        for c in df_str.columns:
            col_unique_val_count = len(iterable[c].unique())
            cardinality = col_unique_val_count / rows
            print(f"{c}: {cardinality}")


# TODO Create a describe stats helper that adds additional summary stats to a described pandas df.
# TODO Add a cardinality row


def describe_stats(df_to_describe):
    """[summary]

    Arguments:
        df {[type]} -- [description]

    Returns:
        [type] -- [description]
    """
    df = df_to_describe.describe()
    df.loc["cv"] = df.loc["std"] / df.loc["mean"]
    df.loc["iqr"] = df.loc["75%"] - df.loc["25%"]
    # Index order
    stats_index = [
        "count",
        "mean",
        "std",
        "cv",
        "min",
        "25%",
        "50%",
        "75%",
        "max",
        "iqr",
    ]
    stats_df = df.reindex(stats_index)
    return stats_df
