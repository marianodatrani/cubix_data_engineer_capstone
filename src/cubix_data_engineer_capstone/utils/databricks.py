from pyspark.sql import DataFrame, SparkSession


def read_file_from_volume(full_path: str, format: str) -> DataFrame:
    """Reads a file from UC Volume and returns it as a DataFrame.

    Args:
        full_path (str): The path to the file in the volume.
        format (str): The format of the file. Can be "csv", "parquet", "delta".

    Returns:
        DataFrame: DataFrame with the data.
    """
    if format not in ["csv", "parquet", "delta"]:
        raise ValueError(f"Invalid format {format}. Supported formats are: csv, parquet, delta.")  # noqa: E501

    spark = SparkSession.getActiveSession()

    reader = spark.read.format(format)
    if format == "csv":
        reader = reader.option("header", "true")

    return reader.load(full_path)
