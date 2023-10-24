from DP_transacciones_total import get_transacciones_dp_total
from general_rangos_de_fecha import get_rangos
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import col, count_distinct
from typing import Literal

def get_activos_en_serie_de_tiempo(session: Session, transacciones_total:DataFrame, serie_de_tiempo:list[Literal['ANIO_ALSEA', 'MES_ALSEA', 'SEM_ALSEA', 'FECHA']], intervalo_de_dias:int = 180) -> DataFrame:
    if ('EMAIL' not in transacciones_total.columns) and ('FECHA' not in transacciones_total.columns):
        raise KeyError('El DataFrame de transacciones debe contener las columnas "EMAIL" y "FECHA"')
    elif 'EMAIL' not in transacciones_total.columns:
        raise KeyError('El DataFrame de transacciones debe contener una columna "EMAIL"')
    elif 'FECHA' not in transacciones_total.columns:
        raise KeyError('El DataFrame de transacciones debe contener una columna "FECHA"')

    rangos = get_rangos(session, serie_de_tiempo, {'FECHA_FIN': 0, 'FECHA_INICIO': -intervalo_de_dias})

    res = (
        transacciones_total
        .join(rangos)
        .filter(col('FECHA_INICIO') <= col('FECHA'))
        .filter(col('FECHA') <= col('FECHA_FIN'))
        .group_by('ANIO_ALSEA', 'SEM_ALSEA')
        .agg(count_distinct('EMAIL').alias('ACTIVOS'))
    )

    return res