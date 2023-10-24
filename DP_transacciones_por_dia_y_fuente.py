from DP_transacciones_total import get_transacciones_dp_total
from snowflake.snowpark.functions import count_distinct
from snowflake.snowpark import Session, DataFrame

def get_transacciones_dp_por_dia_y_fuente(session:Session) -> DataFrame:

    transacciones_dp_total = (
        get_transacciones_dp_total(session)
        .group_by(['FECHA', 'FUENTE'])
        .agg(count_distinct('ORDER_ID'))
        .order_by('FECHA', ascending = False)
    )

    return transacciones_dp_total