from pipelines.stream_sales import validate_orders
import pandas as pd

def test_validate_orders():

    data = {
        "order_id":[1,2,None],
        "price":[10,20,30]
    }

    df = pd.DataFrame(data)

    result = validate_orders(df)

    assert result["order_id"].isnull().sum() == 0