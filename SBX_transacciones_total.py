from snowflake.snowpark import Session, DataFrame

def transacciones_sbx_total(session: Session) -> DataFrame:
    return session.table('SEGMENT_EVENTS.SESSIONM_SBX.FACT_TRANSACTIONS')