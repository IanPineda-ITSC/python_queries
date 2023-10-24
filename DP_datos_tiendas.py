from snowflake.snowpark import Session, DataFrame

def datos_tiendas_dp(session: Session) -> DataFrame:
    return session.table('SIMON_KUCHER.DOMINOS.STORE_MASTER')