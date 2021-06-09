"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from getpairdata import getPools, getPastMinuteData,

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def create_dag(name, addr):

    # grab data from etherscan api
    def extract_op(e_id, **kwargs):
        ti = kwargs['ti']
        raw_tx = getPastMinuteData(addr)
        return raw_tx

    # parse data for swaps, adds, & removes
    def transform_op(e_id, t_id, **kwargs):
        ti = kwargs['ti']
        raw_tx = ti.xcom_pull(key="return_value", task_ids=e_id)
        swaps, adds, removes = transformRaw(raw_tx)
        if len(swaps) == 0: swaps = "None"
        if len(adds) == 0: adds = "None"
        if len(removes) == 0: removes = "None"
        ti.xcom_push(key="swaps", value=swaps)
        ti.xcom_push(key="adds", value=adds)
        ti.xcom_push(key="removes", value=removes)

    # load all swaps etc into mongo
    def load_swaps_op(t_id, **kwargs):
        ti = kwargs['ti']
        swaps = ti.xcom_pull(key="swaps", task_ids=t_id)

        #load into mongo

    # load all adds etc into mongo
    def load_adds_op(t_id, **kwargs):
        ti = kwargs['ti']
        adds = ti.xcom_pull(key="adds", task_ids=t_id)

        #load into mongo

    # load all swaps etc into mongo
    def load_removes_op(t_id, **kwargs):
        ti = kwargs['ti']
        removes = ti.xcom_pull(key="removes", task_ids=t_id)

        #load into mongo

    eid = 'extract_{}'.format(name)
    tid = 'transform_{}'.format(name)
    lsid = 'load_swaps_{}'.format(name)
    laid = 'load_adds_{}'.format(name)
    lrid = 'load_removes_{}'.format(name)
    endid = 'end_{}'.format(name)
    did = 'refresh_pool_{}'.format(name)

    dag = DAG(did, default_args=default_args,
        schedule_interval=timedelta(minutes=1))

    with dag:

        e_op = PythonOperator(
            task_id=eid,
            op_kwargs={e_id: eid},
            python_callable=extract_op)

        t_op = PythonOperator(
            task_id=tid,
            op_kwargs={e_id: eid, t_id: tid}
            python_callable=transform_op)

        ls_op = PythonOperator(
            task_id=lsid,
            op_kwargs={t_id: tid},
            python_callable=load_swaps_op)

        la_op = PythonOperator(
            task_id=laid,
            op_kwargs={t_id: tid},
            python_callable=load_adds_op)

        lr_op = PythonOperator(
            task_id=lrid,
            op_kwargs={t_id: tid},
            python_callable=load_removes_op)

        end_op = DummyOperator(task_id=endid)

        e_op >> t_op >> [ls_op, la_op, lr_op] >> end_op

    return dag

for pool in getPools():
    name = pool[0]
    addr = pool[1]

    dag_id = 'refresh_pool_{}'.format(name)

    globals()[dag_id] = create_dag(name, addr)
