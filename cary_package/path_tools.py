from os.path import getsize
import os


def check_file_exist(filepath: str) -> bool:
    """
    Check if a file exists

    Parameters
    ----------
    filepath : str
        Path of the file to check

    Returns
    -------
    bool
        True if the file exists, False otherwise
    """
    if os.path.isfile(filepath):
        return True
    else:
        return False


def get_folder_file_path_list(folder_path: str) -> list:
    """
    Get a list of file paths in a given folder, sorted by file size in descending order.

    Parameters
    ----------
    folder_path : str
        Path of the folder to get file paths from

    Returns
    -------
    list
        List of file paths in the given folder, sorted by file size in descending order
    """
    result_list = []
    for dirpath, _, filenames in os.walk(folder_path):
        for f in filenames:
            result_list.append(os.path.abspath(os.path.join(dirpath, f)))
        result_list = sorted(result_list, key=getsize, reverse=True)
    return result_list


def get_folders_and_files_by_path(folder_path: str) -> dict:
    """
    Get a dictionary of folders and files in a given folder, sorted by name in descending order.

    Parameters
    ----------
    folder_path : str
        Path of the folder to get folders and files from

    Returns
    -------
    dict
        Dictionary with keys 'folders' and 'files', each containing a list of absolute paths
    """
    result_dict = {'folders': [], 'files': []}

    # 取得根目錄下的所有項目
    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        if os.path.isdir(item_path):
            # 如果是資料夾，加入 folders
            result_dict['folders'].append(os.path.abspath(item_path))
        elif os.path.isfile(item_path):
            # 如果是檔案，加入 files
            result_dict['files'].append(os.path.abspath(item_path))

    # 對資料夾和檔案列表進行降冪排序
    result_dict['folders'].sort(reverse=True)
    result_dict['files'].sort(reverse=True)
    return result_dict


def get_file_size(filepath: str) -> int:
    """
    Get the size of a file in bytes.

    Parameters
    ----------
    filepath : str
        Path of the file to get the size of

    Returns
    -------
    int
        Size of the file in bytes
    """

    return int(os.path.getsize(filepath))


if __name__ == "__main__":
    print("Call it locally")
