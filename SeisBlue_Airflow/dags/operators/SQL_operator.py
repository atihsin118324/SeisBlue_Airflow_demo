from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from sqlalchemy.orm import sessionmaker
from airflow.hooks.mysql_hook import MySqlHook


def get_session(conn_id: str):
    hook = MySqlHook(mysql_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()
    return sessionmaker(bind=engine)()


class SQLAlchemyOperator(PythonOperator):
    @apply_defaults
    def __init__(
            self,
            conn_id: str,
            *args, **kwargs):
        self.conn_id = conn_id

        super().__init__(*args, **kwargs)

    def execute_callable(self):
        session = get_session(self.conn_id)
        try:
            result = self.python_callable(*self.op_args,
                                          session=session,
                                          **self.op_kwargs)
        except Exception:
            session.rollback()
            raise
        session.commit()
        return result
