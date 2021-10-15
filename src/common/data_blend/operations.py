import pandas as pd
from rice.data_blend import Field


def df_apply(df, funcs):
    """
    Applies functions(s) to DataFrame.
    @param df: pandas.DataFrame
    @param funcs: callable function or dict containing column: list of callable functions.
    @return: resulting pandas.DataFrame
    """
    if not callable(funcs) and not isinstance(funcs, dict):
        raise ValueError(f"Expected {', '.join([a.__name__ for a in [callable, dict]])} "
                         f"as argument of {__name__}, got={type(funcs)}")

    if callable(funcs):
        return df.apply(funcs, axis=1)

    for col, operations in funcs.items():
        for operation in operations:
            df[col] = df.apply(operation, axis=1)
    return df


def df_astype(df, types):
    """
    Changes data types of pandas.DataFrame according to provided dictionary as older versions of
    pandas do not support df.astype(dict)
    @param df:
    @param types:
    @return:
    """
    if not isinstance(types, dict):
        raise ValueError(f'Expected {dict.__name__} as argument of {__name__}, got={type(types)}')

    # if int(pd.__version__[2:4]) < 19:
    for col, dtype in types.items():
        df[col] = df[col].astype(dtype)
    else:
        df = df.astype(types)

    return df


def df_drop(df, fields, errors='raise'):
    """
    Generic function that is independent of pandas version to drop columns in a data frame.
    @param df: pandas.DataFrame
    @param fields: dictionary with {original_column: action} or (list, set, str, tuple)
    @param errors: whether to raise or not errors in ase column is missing
    @return: pandas.DataFrame with dropped columns
    """
    if fields is None:
        return df

    elif isinstance(fields, dict):
        return df.drop(
            labels=[k for k, v in fields.items() if v is Field.DROP],
            axis=1,
            errors=errors
        )

    elif isinstance(fields, (list, set, str, tuple)):
        return df.drop(
            fields,
            axis=1,
            errors=errors
        )

    else:
        raise ValueError(f'Unknown structure to drop fields, expected None, dict, list, set, str, or tuple.'
                         f'Got {type(fields)} instead.')


def df_map(df, funcs, na_action=None):
    """
    Maps DataFrame per Series values
    @param df: pandas DataFrame
    @param funcs: callable function or dict containing column: List of callable functions
    @param na_action: action for Na
    @return: resulting pandas.DataFrame
    """
    _na_action = [None, 'ignore']

    if not na_action in _na_action:
        raise ValueError(f"Expected {', '.join([str(a) for a in _na_action])} as argument of na_action.")

    if not callable(funcs) and not isinstance(funcs, dict):
        raise ValueError(f"Expected {', '.join([a.__name__ for a in [callable, dict]])} as argument of {__name__}"
                         f", got={type(funcs)}")

    if callable(funcs):
        return df.map(funcs, na_action)

    for col, operations in funcs.items():
        for operation in operations:
            df[col] = df[col].map(operation, na_action)
    return df


def df_rename(df, fields):
    """
    Generic function to rename columns in DataFrame
    @param df: pandas.DataFrame
    @param fields: dictionary with {original_column: renamed_column}
    @return: renamed DataFrame
    """
    if fields is None:
        return df

    elif isinstance(fields, dict):
        return df.rename(
            columns={k: v for k, v in fields.items() if v not in [None, Field.DROP, Field.KEEP]}
        )

    else:
        raise ValueError(f"Expected dictionary as argument of df_rename, got={type(fields)}")


def df_subset(df, fields):
    """
    Returns a subset of df containing only columns that are keys of dictionary fields.
    @param df: pandas.DataFrame
    @param fields: dictionary of fields or (list, set, str, tuple)
    @return: subset of pandas.DataFrame
    """
    if isinstance(fields, dict):
        return pd.DataFrame(df, columns=fields.keys())

    elif isinstance(fields, (list, tuple, set, str)):
        return pd.DataFrame(df, columns=fields)

    else:
        raise ValueError(f"Unknown structure to do subset, expected dict, list, set, str or tuple. Got={type(fields)}")


def df_prepare(df, fields):
    """
    applies df_subset -> df_drop -> df_rename to DataFrame provided with field settings
    @param df: pandas.DataFrame
    @param fields: dictionary with {original_column: action/rename}
    @return: blended pandas.DataFrame
    """
    return df_rename(df_drop(df_subset(df, fields), fields), fields)

