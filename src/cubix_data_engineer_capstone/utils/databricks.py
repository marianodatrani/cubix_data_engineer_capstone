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


def write_file_to_volume(
        df: DataFrame,
        full_path: str,
        format: str,
        mode: str = "overwrite",
        partition_by: list[str] = None
) -> None:
    """Writes a DataFrame to UC Volume as parquet / csv / delta format.

    Args:
        df (DataFrame): DataFrame to be written.
        full_path (str): The path to the file on the volume.
        format (str): The format of the file ("csv", "json", "delta", "parquet").
        mode (str, optional): Write mode. Defaults to "overwrite".
        partition_by (list[str], optional): List of column to partition by. Defaults to None.

    Raises:
        ValueError: _description_
    """
    if format not in ["csv", "parquet", "delta"]:
        raise ValueError(f"Invalid format {format}. Supported formats are: csv, parquet, delta.")  # noqa: E501

    writer = df.write.mode(mode).format(format)
    if format == "csv":
        writer = writer.option("header", True)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(full_path)
