"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta

from getpairdata import getPools, getDataFromBlock, transformRaw

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

mongoID = "mongo"

def create_dag(name, addr):

    # grab data from etherscan api
    def extract_op(e_id, pname, **kwargs):
        mongoHook = MongoHook(conn_id=mongoID)
        poolLines = mongoHook.find("pools", {"name": pname})
        recent_block = 0
        for pL in poolLines:
            recent_block = pL[u'last_block']
        mongoHook.close_conn()

        # need to make this a HTTPoperator
        raw_tx = getDataFromBlock(addr, recent_block)
        print(recent_block, raw_tx)
        return raw_tx

    # parse data for swaps, adds, & removes
    def transform_op(e_id, t_id, **kwargs):
        ti = kwargs['ti']
        raw_tx = ti.xcom_pull(key="return_value", task_ids=e_id)
        list_tx, lb = transformRaw(raw_tx)
        if len(list_tx) == 0: list_tx = "None"
        ti.xcom_push(key="txs", value=list_tx)
        ti.xcom_push(key="lb", value=lb)

    # load all tx into mongo
    def load_op(t_id, p_name, **kwargs):
        ti = kwargs['ti']
        txs = ti.xcom_pull(key="txs", task_ids=t_id)
        lb  = ti.xcom_pull(key="lb", task_ids=t_id)
        print(txs, lb)

        #load into mongo
        lDict = list()
        for tx in txs:
            lDict.append({"txType": tx[0], "timestamp": tx[1], "price":tx[2], "vol":tx[3]})

        mongoHook = MongoHook(conn_id=mongoID)
        mongoHook.insert_many(p_name, lDict)
        mongoHook.update_one("pools", {"name": p_name}, {"$set":{"last_block":lb}})
        mongoHook.close_conn()

    # clear out xcoms
    @provide_session
    def cleanup_xcom(context, session=None):
        dag_id = context["ti"]["dag_id"]
        session.query(XCom).filter(XCom.dag_id == dag_id).delete()

    eid = 'extract_{}'.format(name)
    tid = 'transform_{}'.format(name)
    lid = 'load_{}'.format(name)
    endid = 'end_{}'.format(name)
    koid = 'kicking_off_{}'.format(name)
    did = 'refresh_pool_{}'.format(name)

    dag = DAG(did, default_args=default_args,
        schedule_interval=timedelta(minutes=1),
        on_success_callback=cleanup_xcom)

    with dag:

        ko_op = DummyOperator(task_id=koid)

        e_op = PythonOperator(task_id=eid,
            op_kwargs={"e_id": eid, "pname": name},
            python_callable=extract_op)

        t_op = PythonOperator(task_id=tid,
            op_kwargs={"e_id": eid, "t_id": tid},
            python_callable=transform_op)

        l_op = PythonOperator(task_id=lid,
            op_kwargs={"t_id": tid, "p_name": name},
            python_callable=load_op)

        end_op = DummyOperator(task_id=endid)

        ko_op >> e_op >> t_op >> l_op >> end_op

    return dag

for pool in getPools():
    name = pool[0]
    addr = pool[1]

    dag_id = 'refresh_pool_{}'.format(name)

    globals()[dag_id] = create_dag(name, addr)
