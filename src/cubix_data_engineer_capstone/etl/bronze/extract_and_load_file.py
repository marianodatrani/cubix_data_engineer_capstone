from cubix_data_engineer_capstone.utils.databricks import read_file_from_volume, write_file_to_volume


def bronze_ingest_volume(
        source_path: str,
        bronze_path: str,
        file_name: str,
        partition_by: list[str] = None
):
    """Extract files from the source, and load them to the bronze volume.

    Args:
        source_path (str): Path to source file.
        bronze_path (str): Path to the bronze layer.
        file_name (str): Name of the file to ingest.
        partition_by (list[str], optional): Column(s) to partition on. Defaults to None.

    Returns:
        None
            Writes the DataFrame to the specified UC Volume path.
    """
    df = read_file_from_volume(f"{source_path}/{file_name}", "csv")

    return write_file_to_volume(
        df=df,
        full_path=f"{bronze_path}/{file_name}",
        format="csv",
        mode="overwrite",
        partition_by=partition_by
    )
