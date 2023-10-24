from snowflake.snowpark.functions import col, substring, lower, concat
from snowflake.snowpark import Session, DataFrame   

def get_transacciones_cloud(session: Session) -> DataFrame:
    transacciones_cloud = (
        session.table('SEGMENT_EVENTS.DOMINOS_GOLO.VENTA_CLOUD')
        .filter(~col('STOREID').like('9%'))
        .filter(col('SOURCEORGANIZATIONURI').is_not_null())
        .filter(col('SOURCEORGANIZATIONURI').isin(['order.dominos.com', 'resp-order.dominos.com', 'iphone.dominos.mx', 'android.dominos.mx']))
        .with_column('FECHA', substring(col('STOREORDERID'), 1, 10))
        .with_column('EMAIL', lower(col('EMAIL')))
        .with_column('ORDER_ID', concat(col('FECHA'), col('STOREID'), col('STOREORDERID')))
        .with_column('VENTA', col('PAYMENTSAMOUNT') / 1.16)
        .select(['EMAIL', 'FECHA', 'VENTA', 'ORDER_ID', 'PHONE'])
    )

    return transacciones_cloud