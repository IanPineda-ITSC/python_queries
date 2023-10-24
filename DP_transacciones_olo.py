from snowflake.snowpark.functions import col, lower, upper, to_char, concat
from snowflake.snowpark import Session, DataFrame

def get_transacciones_olo(session: Session) -> DataFrame:
    mx_power_sales_log = (
        session
        .table('SEGMENT_EVENTS.DOMINOS_OLO.MXPOWERSALESLOG')
        .with_column_renamed(col('ORDERNUMBER'), 'ORDER_NUMBER')
        .with_column_renamed(col('STORENUMBER'), 'LOCATION_CODE')
        .with_column_renamed(col('ORDERDATE'), 'FECHA')
        .with_column('EMAIL', lower(col('EMAIL')))
    )

    dpm_sales_full = (
        session
        .table('SEGMENT_EVENTS.DOMINOS_OLO.DPMSALES_FULL')
        .with_column_renamed(col('ORDER_DATE'), 'FECHA')
        .with_column('SOURCE_CODE', upper(col('SOURCE_CODE')))
        .filter(col('ORDER_STATUS_CODE') == 4)
        .filter(~col('LOCATION_CODE').isin(['13001', '13006', '13021', '11000']))
        .filter(col('SOURCE_CODE').isin(['ANDROID' , 'DESKTOP', 'IOS', 'MOBILE', 'WEB', 'ANDROID2', 'DESKTOP2', 'IOSAPP', 'MOBILE2', 'WHATSAPP']))
    )

    transacciones_olo = (
        mx_power_sales_log
        .join(dpm_sales_full, on = ['ORDER_NUMBER', 'LOCATION_CODE','FECHA'])
        .with_column('VENTA', col('ORDERFINALPRICE') / 1.16)
        .with_column('ORDER_ID', concat(to_char(col('FECHA')), col('LOCATION_CODE'), col('ORDER_NUMBER')))
        .select(['EMAIL', 'FECHA', 'VENTA', 'ORDER_ID', col('PHONENUMBER').alias('PHONE')])
    )

    return transacciones_olo