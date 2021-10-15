import glob
import os


def most_recent_file(path, pattern="*", level=0, key=os.path.getctime):
    """
    Returns the path of most recent file inside a directory or raises a value error if path contains no file/path.
    @param path: directory to find files
    @param pattern: optional: filter files in directory by pattern
    @param level: optional: search in n-levels of sub folders
    @param key: key used to find the most recent file (default: modification time)
    @return: full path to most recent file inside provided directory
    """
    try:
        return max(glob.glob(os.path.join(path, '*/' * level, pattern)), key=key)
    except ValueError as e:
        raise ValueError("No files found at this path/level") from e


def most_recent_folder(path, pattern="*", key=os.path.getctime):
    """
    Returns the path of most recent folder inside a directory or raises a ValueError if path contains no file/path.
    @param path: directory to find folders
    @param pattern: optional: filter folders in directory by pattern
    @param key: key used to find the most recent folder (default: modification time)
    @return: full path to most recent folder inside provided directory
    """
    try:
        return max(glob.glob(os.path.join(path, f"{pattern}/")), key=key)
    except ValueError as e:
        raise ValueError("No files found at this path/level") from e

