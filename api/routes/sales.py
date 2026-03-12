from fastapi import APIRouter
from api.bigquery_client import run_query

router = APIRouter()

@router.get("/top-categories")
def get_top_categories():

    query = """
    SELECT
        category,
        SUM(total_sales) as total_sales
    FROM `meli-data-platform.meli_analytics.sales_by_category_hourly`
    GROUP BY category
    ORDER BY total_sales DESC
    LIMIT 10
    """

    return run_query(query)