from pyspark.sql import DataFrame


def _join_master_tables(
        sales_master: DataFrame,
        calendar_master: DataFrame,
        customers_master: DataFrame,
        products_master: DataFrame,
        product_subcategory_master: DataFrame,
        product_category_master: DataFrame
) -> DataFrame:
    """Join the master DataFrames to the Sales Master.
    Drop Date, ProductSubCategoryKey and ProductCategoryKey to avoid duplicate columns.

    Args:
        sales_master (DataFrame): Master DataFrames for Sales.
        calendar_master (DataFrame): Master DataFrames for Calendar.
        customers_master (DataFrame): Master DataFrames for Customers.
        products_master (DataFrame): Master DataFrames for Products.
        product_subcategory_master (DataFrame): Master DataFrames for Product Subcategory.
        product_category_master (DataFrame): Master DataFrames for Product Category.

    Returns:
        DataFrame: Sales Master with all the joined DataFrames.
    """

    return (
        sales_master
        .join(calendar_master, sales_master["OrderDate"] == calendar_master["Date"], how="left")
        .drop(calendar_master["Date"])
        .join(customers_master, on="CustomerKey", how="left")
        .join(products_master, on="ProductKey", how="left")
        .join(
            product_subcategory_master,
            products_master["ProductSubCategoryKey"] == product_subcategory_master["ProductSubCategoryKey"],
            how="left"
            )
        .drop(product_subcategory_master["ProductSubCategoryKey"])
        .join(
            product_category_master,
            product_category_master["ProductCategoryKey"] == product_subcategory_master["ProductCategoryKey"],
            how="left"
            )
        .drop(product_category_master["ProductCategoryKey"])
    )
