from enum import Enum
from itertools import combinations

import networkx as nx
import numpy as np
import pandas as pd
from textdistance import (
    cosine,
    damerau_levenshtein,
    jaccard,
    jaro_winkler,
    levenshtein,
)
from tqdm import tqdm

from utils import UtilsDQAnalysis as DataQualityUtils


class MatchingAlgorithm(Enum):
    LEVENSHTEIN = "levenshtein"
    DAMERAU_LEVENSHTEIN = "damerau_levenshtein"
    JACCARD = "jaccard"
    JAROWINKLER = "jaro_winkler"
    COSINE = "cosine"


def compute_similarity_matrix(
    df,
    label_column,
    matching_threshold,
    matching_algorithms=[MatchingAlgorithm.LEVENSHTEIN],
):
    """
    Compute the similarity between string values using multiple algorithms.

    Parameters:
    ----------
    df (pd.DataFrame): The input DataFrame containing labels and other data.
    label_column (str): The column name in the DataFrame containing labels.
    matching_threshold (float): The threshold for similarity scores to consider.
    matching_algorithms (list): List of MatchingAlgorithm enums to use for matching.

    Returns:
    --------
    pd.DataFrame: A DataFrame containing matched labels.
    """
    algorithm_mapping = {
        MatchingAlgorithm.LEVENSHTEIN: levenshtein,
        MatchingAlgorithm.DAMERAU_LEVENSHTEIN: damerau_levenshtein,
        MatchingAlgorithm.JACCARD: jaccard,
        MatchingAlgorithm.JAROWINKLER: jaro_winkler,
        MatchingAlgorithm.COSINE: cosine,
    }

    matching_functions = [
        algorithm_mapping.get(algo) for algo in matching_algorithms
    ]
    print(
        f"Using matching_algorithms = {matching_algorithms} with matching_threshold = {matching_threshold}"
    )

    if None in matching_functions:
        raise ValueError(
            "One or more of the specified algorithms are not known or not implemented yet!"
        )

    unique_labels = df[label_column].unique()
    label_pairs = list(combinations(unique_labels, 2))

    label1_array, label2_array = zip(*label_pairs)
    similarities = np.array(
        [
            max(
                [
                    matching_function.normalized_similarity(label1, label2)
                    for matching_function in matching_functions
                ]
            )
            for label1, label2 in tqdm(label_pairs)
        ]
    )

    mask = similarities >= matching_threshold
    filtered_label_pairs = [
        (label1_array[i], label2_array[i], similarities[i])
        for i in np.where(mask)[0]
    ]

    # Create a DataFrame for filtered label pairs and their similarity scores
    similarity_matrix = pd.DataFrame(
        filtered_label_pairs, columns=["label1", "label2", "Final_score"]
    )
    similarity_matrix["Combinations"] = similarity_matrix[
        ["label1", "label2"]
    ].apply(list, axis=1)
    similarity_matrix.drop(columns=["label1", "label2"], inplace=True)
    print(f"Found {len(similarity_matrix)} matches")
    return similarity_matrix


def create_matched_dataframe(
    df,
    label_column,
    matching_threshold,
    matching_algorithms,
):
    """
    Compute and return a DataFrame with matched and unmatched labels using multiple algorithms.

    Parameters:
    ----------
    df (pd.DataFrame): The input DataFrame containing labels and other data.
    label_column (str): The column name in the DataFrame containing labels.
    matching_threshold (float): The threshold for similarity scores to consider.
    matching_algorithms (list): List of MatchingAlgorithm enums to use for matching.

    Returns:
    --------
    pd.DataFrame: A DataFrame containing matched and unmatched labels.
    """
    working_df = (
        df.dropna(subset=["label"])
        .drop_duplicates(subset=["label"])
        .reset_index(drop=True)
    )
    similarity_df = compute_similarity_matrix(
        working_df, label_column, matching_threshold, matching_algorithms
    )

    if not similarity_df.empty:
        matched_labels = list(similarity_df.Combinations)
        matched_df = pd.DataFrame()
        for i in range(len(matched_labels)):
            df_tmp = working_df.loc[
                (working_df["label"].isin(matched_labels[i])), :
            ].copy()
            df_tmp["_cluster_id"] = DataQualityUtils.id_generator()
            dfs = [matched_df, df_tmp]
            matched_df = pd.concat(dfs, ignore_index=True)

        flattened_matched_labels = [
            item for sublist in matched_labels for item in sublist
        ]
        result_df = working_df[
            ~working_df["label"].isin(flattened_matched_labels)
        ]
        result_df = pd.concat([result_df, matched_df]).fillna("")
        result_df["_cluster_id"] = result_df["_cluster_id"].apply(
            lambda x: DataQualityUtils.id_generator() if x == "" else x
        )
        return result_df
    else:
        raise ValueError("No match was found")


def process_clusters(clustered_df, main_df):
    grouped_clusters_df = (
        clustered_df.groupby("_cluster_id")["_unicity_key"]
        .apply(set)
        .reset_index(name="_unicity_key_list")
    )
    clustered_df = DataQualityUtils.merge_dataframes(
        left_df=clustered_df,
        right_df=grouped_clusters_df,
        merge_kwargs={"on": "_cluster_id", "how": "left"},
    )

    # Create a graph and add nodes and edges based on connected '_unicity_key_list' sets
    graph = nx.Graph()
    for cluster_keys in clustered_df["_unicity_key_list"]:
        graph.add_nodes_from(cluster_keys)
        graph.add_edges_from(combinations(cluster_keys, 2))

    # Find connected components in the graph
    connected_clusters = list(nx.connected_components(graph))

    # Map '_unicity_key' values to cluster IDs
    key_to_cluster_id = {
        key: f"cluster_{i}"
        for i, cluster in enumerate(connected_clusters)
        for key in cluster
    }
    clustered_df["_cluster_id"] = clustered_df["_unicity_key"].map(
        key_to_cluster_id
    )

    # Create a dictionary to map labels to cluster IDs
    label_to_cluster_id = dict(
        zip(clustered_df.label, clustered_df._cluster_id)
    )

    # Map cluster IDs to the main DataFrame
    main_df["_cluster_id"] = main_df["label"].map(label_to_cluster_id)
    main_df["_cluster_size"] = main_df.groupby("_cluster_id")[
        "_cluster_id"
    ].transform("count")
    return main_df
