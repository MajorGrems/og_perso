import time
from enum import Enum

import pandas as pd

from utils import UtilsDQAnalysis as DataQualityUtils
from utils import UtilsMatching as MatchingUtils


class DataSource(Enum):
    CLEAR = "clear"
    CCAP = "ccap"


class DataProcessor:
    def __init__(
        self,
        data_source,
        matching_algorithms=[
            MatchingUtils.MatchingAlgorithm.LEVENSHTEIN,
            MatchingUtils.MatchingAlgorithm.JACCARD,
        ],
        matching_threshold=0.9,
    ):
        """
        Initialize a DataProcessor for processing data from a specified data source.

        Parameters
        ----------
        data_source (str): The source of the data to process.
        matching_algorithms (MatchingAlgorithm): List of algorithms to use for matching, defaults to : [MatchingAlgorithm.LEVENSHTEIN, MatchingUtils.MatchingAlgorithm.JACCAR].
        matching_threshold (float): The threshold for similarity scores to consider, defaults to 0.9.
        """
        self.data_source = data_source
        self.matching_threshold = matching_threshold
        self.matching_algorithms = matching_algorithms
        self.source_file_path = self._get_source_file_path()
        self.read_data_kwargs = self._get_read_data_kwargs()
        self.df_processed = None  # Processed data will be stored here.

    def _get_source_file_path(self):
        """Returns the path of the input file depending on the data source."""
        source_file_paths = {
            DataSource.CLEAR: "./data/suez/CLEAR - référentiel services -20230811.xlsx",
            DataSource.CCAP: "./data/suez/CCAP Entreprises - Kit catalogue national - 07072023.xlsx",
        }
        return source_file_paths[self.data_source]

    def _get_read_data_kwargs(self):
        """Returns the kwargs to use for reading the input file depending on the data source."""
        read_data_kwargs = {
            DataSource.CLEAR: {
                "sheet_name": "services et RedMaj",
                "skiprows": 1,
                "usecols": [
                    "Activité",
                    "Code Service",
                    # "Code service fils",
                    "Libellé du service",
                    "Libellé facture du service",
                ],
            },
            DataSource.CCAP: {
                "sheet_name": "Export 110723 - National",
                "skiprows": 2,
                "usecols": [
                    "CODE PRESTATION",
                    "LIBELLE INTERNE",
                    "LIBELLE COMMERCIAL",
                    "Modélisation",
                    "Marge Brute en %",
                ],
            },
        }
        return read_data_kwargs[self.data_source]

    def load_data(self):
        """Loads the data from the input file depending on the data source."""
        self.df_processed = DataQualityUtils.read_data(
            file_path=self.source_file_path,
            read_function=pd.read_excel,
            read_kwargs=self.read_data_kwargs,
        )
        self.df_processed[
            "_unicity_key"
        ] = self.df_processed.index  # Add unicity id
        print("Shape : ", self.df_processed.shape)
        if self.data_source == DataSource.CLEAR:
            self.df_processed = self.df_processed.rename(
                columns={
                    "Libellé du service": "LIBELLE INTERNE",
                    "Activité": "Modélisation",
                    "Code Service": "CODE PRESTATION",
                    "Libellé facture du service": "LIBELLE COMMERCIAL",
                }
            )  # Rename column to harmonize with CCAP
            # self.df_processed["CODE PRESTATION"] = self.df_processed.apply(
            #     lambda row: f"{row['Modélisation']} | {row['Code Service']} | {row['Code service fils']}",
            #     axis=1,
            # )
            self.df_processed = self.df_processed[
                [
                    "CODE PRESTATION",
                    "Modélisation",
                    "LIBELLE INTERNE",
                    "LIBELLE COMMERCIAL",
                    "_unicity_key",
                ]
            ]
            self.df_processed["SOURCE"] = DataSource.CLEAR.value.upper()

        elif self.data_source == DataSource.CCAP:
            self.df_processed["SOURCE"] = DataSource.CCAP.value.upper()
        self.df_processed["LIBELLE COMMERCIAL"] = self.df_processed[
            "LIBELLE COMMERCIAL"
        ].fillna(self.df_processed["LIBELLE INTERNE"])

    def cleanse_data(self):
        """Cleanses the data depending on the data source."""
        if self.data_source == DataSource.CCAP:
            self.df_processed["label"] = self.df_processed[
                "LIBELLE COMMERCIAL"
            ].map(DataQualityUtils.process_label_ccap)

        elif self.data_source == DataSource.CLEAR:
            self.df_processed["label"] = self.df_processed[
                "LIBELLE COMMERCIAL"
            ].map(DataQualityUtils.process_label_clear)
            # Generate a new column with the reason for rejection
            self.df_processed.loc[
                self.df_processed["label"].str.contains(
                    "ne plus utiliser", case=False
                ),
                "Raison Rejet",
            ] = "NE PLUS UTILISER"
            self.df_processed.loc[
                self.df_processed["label"].str.contains("dryrun", case=False),
                "Raison Rejet",
            ] = "DRYRUN"
            # Generate a new dataframe with rejected rows
            self.rejected_data_df = pd.concat(
                [
                    self.df_processed[
                        self.df_processed["Raison Rejet"] == "NE PLUS UTILISER"
                    ],
                    self.df_processed[
                        self.df_processed["Raison Rejet"] == "DRYRUN"
                    ],
                ]
            )
            # Remove rejected rows from the main dataframe
            self.df_processed = self.df_processed[
                ~self.df_processed["label"].str.contains(
                    "ne plus utiliser", case=False
                )
            ]
            self.df_processed = self.df_processed[
                ~self.df_processed["label"].str.contains("dryrun", case=False)
            ]
        self.dict_label = {
            val.lower(): val for val in self.df_processed["label"]
        }
        self.df_processed["label"] = self.df_processed["label"].str.lower()

    def run_dq_analysis(self):
        """Runs data quality analysis on the data: completeness, uniqueness, pattern, and length."""
        columns_to_analyze = ["LIBELLE COMMERCIAL", "label"]
        self.df_processed = DataQualityUtils.calculate_completeness(
            df=self.df_processed, columns=columns_to_analyze
        )  # Compute completeness of selected columns
        self.df_processed = DataQualityUtils.calculate_uniqueness(
            df=self.df_processed, columns=columns_to_analyze
        )  # Compute uniqueness of selected columns

        for c in columns_to_analyze:
            self.df_processed["_" + c + "_pattern"] = self.df_processed[c].map(
                DataQualityUtils.generate_pattern
            )  # Generate a pattern for selected columns
            self.df_processed["_" + c + "_len"] = [
                len(str(x))
                if x is not None and not isinstance(x, float)
                else None
                for x in self.df_processed[c]
            ]  # Compute the length of selected columns

    def cluster_data(self):
        """Clusters the data using the selected algorithm and threshold."""
        clustered_data_df = MatchingUtils.create_matched_dataframe(
            df=self.df_processed,
            label_column="label",
            matching_algorithms=self.matching_algorithms,
            matching_threshold=self.matching_threshold,
        )
        self.df_processed = MatchingUtils.process_clusters(
            clustered_df=clustered_data_df, main_df=self.df_processed
        )

    def rename_clusters(self):
        """Renames the clusters by finding the common words in each cluster's label."""
        self.df_processed["labelpro"] = self.df_processed["label"].map(
            self.dict_label
        )
        cluster_label_data = (
            self.df_processed.groupby("_cluster_id")["labelpro"]
            .agg(list_label=list)
            .reset_index()
        )  # Group by cluster id and aggregate labels in a list
        cluster_label_data["final_label"] = cluster_label_data[
            "list_label"
        ].apply(DataQualityUtils.find_common_words_with_order)

        cluster_label_data["final_label"] = cluster_label_data[
            "final_label"
        ].map(DataQualityUtils.postprocess_final_label)

        self.df_processed = DataQualityUtils.merge_dataframes(
            left_df=cluster_label_data[["final_label", "_cluster_id"]],
            right_df=self.df_processed,
            merge_kwargs={"on": "_cluster_id", "how": "left"},
        )

        self.df_processed["labelpro"] = self.df_processed["labelpro"].map(
            DataQualityUtils.postprocess_final_label
        )

        if self.data_source == DataSource.CLEAR:
            # Reinject rejected rows
            self.df_processed = pd.concat(
                [self.df_processed, self.rejected_data_df]
            )

    def process(self):
        """Process the data with all the methods above."""
        print(f"\n========= Processing : {self.data_source} =========\n")
        print(
            f"\n========= Loading ========={round(((time.time() - timer) / 60), 2)} min"
        )
        self.load_data()
        print(
            f"\n========= Cleansing ========={round(((time.time() - timer) / 60), 2)} min"
        )
        self.cleanse_data()
        print(
            f"\n========= Data quality analysis ========={round(((time.time() - timer) / 60), 2)} min"
        )
        self.run_dq_analysis()
        print(
            f"\n========= Clustering ========={round(((time.time() - timer) / 60), 2)} min \n"
        )
        self.cluster_data()
        print(
            f"\n========= Renaming clusters ========={round(((time.time() - timer) / 60), 2)} min \n"
        )
        self.rename_clusters()


class DataExporter:
    def __init__(self, data_to_export):
        """
        Initialize a DataExporter for exporting processed data.

        Parameters
        ----------
        data_to_export (pd.DataFrame): The processed data to export.
        """
        self.exported_file_path = "./data/suez/suez_catalogs_dqAnalysis.xlsx"
        self.data_to_export = data_to_export

    def export_data(self):
        """Export the data"""
        print(
            f"\n========= Writing data ========={round(((time.time() - timer) / 60), 2)} min \n"
        )
        DataQualityUtils.write_data(
            df_output=self.data_to_export,
            file_path=self.exported_file_path,
            write_function=pd.DataFrame.to_excel,
            write_kwargs={"index": False},
        )
        print(
            f"\n========= END ========={round(((time.time() - timer) / 60), 2)} min \n"
        )


if __name__ == "__main__":
    timer = time.time()
    final_df = pd.DataFrame()
    data_sources = [DataSource.CCAP, DataSource.CLEAR]

    for data_source in data_sources:
        if data_source == DataSource.CLEAR:
            matching_threshold = 0.96
        else:
            matching_threshold = 0.93
        data_processor = DataProcessor(
            data_source=data_source,
            matching_threshold=matching_threshold,
            matching_algorithms=[
                MatchingUtils.MatchingAlgorithm.JACCARD,
                MatchingUtils.MatchingAlgorithm.DAMERAU_LEVENSHTEIN,
                MatchingUtils.MatchingAlgorithm.JAROWINKLER,
            ],
        )
        data_processor.process()
        df_processed = data_processor.df_processed
        print(f"Originally {df_processed.shape[0]} labels")
        print(f"Found {df_processed['_cluster_id'].nunique()} clusters")
        final_df = pd.concat([final_df, df_processed], ignore_index=True)

    data_exporter = DataExporter(final_df)
    data_exporter.export_data()
