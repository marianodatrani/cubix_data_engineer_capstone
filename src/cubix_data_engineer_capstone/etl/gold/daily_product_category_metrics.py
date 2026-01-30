import pyspark.sql.functions as sf
from pyspark.sql import DataFrame


def get_daily_product_category_metrics(wide_sales: DataFrame) -> DataFrame:
    """
    Calculates product category metrics from the wide_sales DataFrame.

    Note: In order to get only two decimals for the averages, value is rounded.

    Args:
        wide_sales (DataFrame): Input DataFrame containing wide sales data.

    Returns:
        DataFrame: DataFrame with product category metrics including "SalesAmountSum", "SalesAmountAvg",
                        "ProfitSum", and "ProfitAvg" grouped by "EnglishProductCategoryName".
    """

    return (
        wide_sales
        .groupBy(sf.col("EnglishProductCategoryName"))
        .agg(
            sf.sum(sf.col("SalesAmount")).alias("SalesAmountSum"),
            sf.round(sf.avg(sf.col("SalesAmount")), 2).alias("SalesAmountAvg"),
            sf.sum(sf.col("Profit")).alias("ProfitSum"),
            sf.round(sf.avg(sf.col("Profit")), 2).alias("ProfitAvg"),
        )
    )
