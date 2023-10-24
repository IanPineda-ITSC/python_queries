from DP_transacciones_cloud import get_transacciones_cloud
from DP_transacciones_olo import get_transacciones_olo
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import lit

def get_transacciones_dp_total(session: Session) -> DataFrame:
    transacciones_olo = (
        get_transacciones_olo(session)
        .with_column('FUENTE', lit('OLO'))
    )
    transacciones_cloud = (
        get_transacciones_cloud(session)
        .with_column('FUENTE', lit('CLOUD'))
    )

    transacciones_total = (
        transacciones_cloud
        .union_all(transacciones_olo)
    )

    return transacciones_total