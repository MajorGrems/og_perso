import random
import re
import string
import unicodedata

import pandas as pd


def read_data(file_path, read_function, read_kwargs=None):
    """
    Read data from a file using a specified read function.

    Args:
        file_path (str): The path to the file to read.
        read_function (callable): A function that reads data from a file.
        read_kwargs (dict, optional): Keyword arguments to be passed to the read_function, default: None.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the read data.

    Raises:
        ValueError: If there is an error during file reading or if the file is empty.
    """
    if read_kwargs is None:
        read_kwargs = {}

    try:
        df_input = read_function(file_path, **read_kwargs)
    except Exception as e:
        raise ValueError(f"Error while reading the file '{file_path}': {e}")

    if df_input.shape[0] == 0:
        raise ValueError(f"The file '{file_path}' is empty")

    return df_input


def write_data(df_output, file_path, write_function, write_kwargs=None):
    """
    Write a DataFrame to a file using a specified write function.

    Args:
        data_frame (pd.DataFrame): The DataFrame to be written.
        file_path (str): The path to the file to write.
        write_function (callable): A function that writes data to a file.
        write_kwargs (dict, optional): Keyword arguments to be passed to the write_function, default: None.

    Returns:
        pd.DataFrame: The input DataFrame.
    """
    if write_kwargs is None:
        write_kwargs = {}

    write_function(df_output, file_path, **write_kwargs)
    return df_output


def merge_dataframes(left_df, right_df, merge_kwargs={}):
    """
    Merge two DataFrames using pandas merge function.

    Parameters:
    - left_df (pd.DataFrame): The left DataFrame for merging.
    - right_df (pd.DataFrame): The right DataFrame for merging.
    - merge_kwargs (dict, optional): Keyword arguments to be passed to pd.merge.

    Returns:
    - pd.DataFrame: The merged DataFrame.

    """
    merged_df = pd.merge(left_df, right_df, **merge_kwargs)
    return merged_df


def normalize_text(text):
    """
    Normalize a text string by removing accents.

    Parameters:
    - text (str): The input text.

    Returns:
    - str: The normalized text.

    """
    if pd.isna(text) or (text == ""):
        return ""
    normalized_text = unicodedata.normalize("NFD", text)
    normalized_text = normalized_text.encode("ascii", "ignore")
    normalized_text = normalized_text.decode("utf-8")
    return normalized_text


def generate_pattern(text):
    """
    Apply a pattern transformation to a text string.
        - lowercase letters are replaced by 'x'
        - uppercase letters are replaced by 'X'
        - digits are replaced by '9'

    Parameters:
    - text (str): The input text.

    Returns:
    - str: The generated pattern.

    """
    text = normalize_text(text)

    if text == "":
        return ""

    pattern_tokens = {"[a-z]": "x", "[A-Z]": "X", "[0-9]": "9"}
    generated_pattern = text.lower()
    for pattern, replacement in pattern_tokens.items():
        generated_pattern = re.sub(pattern, replacement, generated_pattern)
    return generated_pattern


def id_generator(size=6):
    """
    Generate a random alphanumeric ID of a specified size.

    Parameters:
    - size (int, optional): The length of the generated ID, default: 6.

    Returns:
    - str: The generated ID.

    """
    characters = (
        string.ascii_lowercase + string.ascii_uppercase + string.digits
    )
    return "".join(random.choice(characters) for _ in range(size))


def process_label_ccap(label):
    if pd.isna(label) or (label == ""):
        return ""

    # replacement_dict = {
    #     "- Solo": "",
    #     "Solo": "",
    #     "- Clear": "",
    #     "- MKGT CF": "",
    #     "- ALEA": "",
    #     "- MKGT": "",
    #     "Rolls": "Roll",
    #     "DIB": "DNDAE",
    # }
    label = normalize_text(label)
    # for key, value in replacement_dict.items():
    #     label = label.replace(key, value)
    label = re.sub(r"(?<=\d)[,.](?=\d)", "", label)
    label = re.sub(r"\b(\d+)\s+(\d+)\b", r"\1\2", label)
    # label = re.sub(r"\d+", "X", label)
    label = re.sub(r"(?<!K)\d+", "X", label)
    # label = re.sub(r"-?\s*[a-zA-Z]+\s*\(X\)", " ", label)
    # label = re.sub(r"\s*-?\s*(?i:(externe|interne))\s*-?\s.*", "", label)
    # label = re.sub(
    #     r"(rotation ampliroll [A-Za-z \']+-)([A-Za-z \']+)", r"\1", label
    # )
    # label = re.sub(r"(Rotation Ampliroll ISDN).*", r"\1", label)
    # label = re.sub(
    #     r"(rotation Ampliroll PLATEFORME DID).*",
    #     r"\1",
    #     label,
    #     flags=re.IGNORECASE,
    # )
    # label = re.sub(r"\([^)]*\)", " ", label)
    label = re.sub(r"[^a-zA-Z0-9\s%]", " ", label)
    # label = re.sub(
    #     r"(ZONE OUEST|ZONE NORD|ZONE SUD|ZONE EST| ZONE NORD EST|ZONE NORD OUEST|ZONE SUD EST|ZONE SUD OUEST|NORD|SUD|EST|OUEST)",
    #     " ",
    #     label,
    #     flags=re.IGNORECASE,
    # )
    label = re.sub(r"\s+", " ", label)
    label = label.strip()
    return label


def process_label_clear(label):
    if pd.isna(label) or (label == ""):
        return ""

    label = normalize_text(label)
    label = re.sub(r"(?<=\d)[,.](?=\d)", "", label)
    label = re.sub(r"\b(\d+)\s+(\d+)\b", r"\1\2", label)
    # label = re.sub(r"\d+", "X", label)
    label = re.sub(r"(?<!K)\d+", "X", label)
    # label = re.sub(r"-?\s*[a-zA-Z]+\s*\(X\)", " ", label)
    # label = re.sub(r"\([^)]*\)", " ", label)
    label = re.sub(r"[^a-zA-Z0-9\s%]", " ", label)
    # label = re.sub(
    #     r"(a la tonne|de la tonne|externe|interne|a la piece|au kilo|a l heure|au metre cube|tonne|la tonne)",
    #     " ",
    #     label,
    #     flags=re.IGNORECASE,
    # )

    label = re.sub(r"\s+", " ", label)
    label = label.strip()
    label = re.sub(r"\bX\s+mX\b", "xmx", label)
    label = re.sub(r"\bX\s+l\b", "xL", label)
    return label


def calculate_completeness(df, columns=None):
    """
    Create completeness columns for each attribute:
        - Completed if exist
        - Missing otherwise
    """
    if columns is None:
        columns = [x for x in df.columns if not x.startswith("_")]
    for column in columns:
        df["_" + column + "_completeness"] = "Completed"
        df.loc[
            (pd.isna(df[column]) | (df[column] == "")),
            "_" + column + "_completeness",
        ] = "Missing"
    return df


def calculate_uniqueness(df, columns=None):
    """
    Create Uniqueness column for each attribute:
        - "Unique" if value is unique
        - "Duplicates" otherwise
    """
    if columns is None:
        columns = [x for x in df.columns if not x.startswith("_")]
    for column in columns:
        value_counts = df[column].value_counts()
        df["_" + column + "_uniqueness"] = df[column].map(value_counts)
        df["_" + column + "_uniqueness"] = df[
            "_" + column + "_uniqueness"
        ].apply(lambda x: "Unique" if x == 1 else "Duplicates")
    return df


def find_common_words_with_order(string_list):
    # Convert all strings to lowercase for comparison
    lower_string_list = [string.lower() for string in string_list]

    # Split each lowercase string into words and convert them to sets
    word_sets = [set(string.split()) for string in lower_string_list]

    # Find the intersection of all sets
    common_words = set.intersection(*word_sets)

    # Create a list of common words in the order of the first string
    final_list = [
        word for word in string_list[0].split() if word.lower() in common_words
    ]

    # Join the common words into a single string
    return " ".join(final_list)


def postprocess_final_label(label):
    if pd.isna(label) or (label == ""):
        return ""
    else:
        label = re.sub(r"\bX\s*mX\b", "Xm3", label, flags=re.I)
        label = re.sub(r"\s?litre[s]?", " L", label, flags=re.I)
        label = re.sub(r"\bX\s*l\b", "XL", label, flags=re.I)
        label = re.sub(r"\bZX\b", "", label)
        label = label.strip()
        return label
