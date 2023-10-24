from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import min, col

def get_primeras_compras(session: Session, transacciones_total:DataFrame) -> DataFrame:

    if ('EMAIL' not in transacciones_total.columns) and ('FECHA' not in transacciones_total.columns):
        raise KeyError('El DataFrame de transacciones debe contener las columnas "EMAIL" y "FECHA"')
    elif 'EMAIL' not in transacciones_total.columns:
        raise KeyError('El DataFrame de transacciones debe contener una columna "EMAIL"')
    elif 'FECHA' not in transacciones_total.columns:
        raise KeyError('El DataFrame de transacciones debe contener una columna "FECHA"')

    tiempo = session.table('WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME')

    primeras_compras = (
        transacciones_total
        .group_by(col('EMAIL'))
        .agg(min('FECHA').alias('FECHA'))
        .join(tiempo, on = 'FECHA')
    ) 

    return primeras_compras