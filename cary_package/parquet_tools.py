import ndjson
import polars as pl
import pandas as pd


def load_pq(file_path, return_df=False, use_pyarrow=True):
    # df = pd.read_parquet(file_path, engine='pyarrow')

    # # 转换为列表字典格式
    # data = df.to_dict(orient='records')

    """
    Load a Parquet file.

    Parameters
    ----------
    file_path : str
        The path to the Parquet file to be loaded.
    return_df : bool, optional
        If True, return a polars DataFrame instead of a list of dictionaries.

    Returns
    -------
    data : list or polars.DataFrame
        A list of dictionaries or a polars DataFrame loaded from the Parquet file.
    """
    try:
        df = pl.read_parquet(file_path, use_pyarrow=True)
    except Exception as e:
        raise Exception(f"load_pq 失敗: {e}")

    if return_df:
        return df
    else:
        data = df.to_dicts()
        return data


def save_pq(file_path, data_list):
    # with open(file_path, 'w') as f:
    #     ndjson.dump(data_list, f, ensure_ascii=False)
    # # 转换为 DataFrame
    # df = pd.DataFrame(data_list)

    # # 保存为 Parquet 文件
    # df.to_parquet(file_path, engine='pyarrow', index=False)

    """
    Save a list of dictionaries to a Parquet file.

    Parameters
    ----------
    file_path : str

        The path where the Parquet file will be saved.
    data_list : list
        A list of dictionaries to be saved as a Parquet file.

    Raises
    ------
    Exception
        If the operation fails, an exception is raised with the error message.
    """

    df = pl.DataFrame(data_list)

    try:
        df.write_parquet(file_path, compression_level=20, use_pyarrow=True)
    except Exception as e:
        raise Exception(f"load_pq 失敗: {e}")
    return
