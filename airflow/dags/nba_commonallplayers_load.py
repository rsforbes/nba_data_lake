import pendulum
from airflow import DAG
from airflow.decorators import task


@task.external_python(
    task_id="get_commonallplayers",
    python="/home/vscode/.cache/pypoetry/virtualenvs/nba-data-lake-5ARuANHH-py3.11/bin/python",
)
def get():
    import nba_data_lake.api.nba as nba

    return nba.get_commonallplayers()


@task.external_python(
    task_id="del_commonallplayers",
    python="/home/vscode/.cache/pypoetry/virtualenvs/nba-data-lake-5ARuANHH-py3.11/bin/python",
)
def delete():
    import nba_data_lake.nba.stats.db.commonallplayers as db
    import asyncio

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(db.CommonAllPlayers().delete())
    return result


@task.external_python(
    task_id="load_commallplayers",
    python="/home/vscode/.cache/pypoetry/virtualenvs/nba-data-lake-5ARuANHH-py3.11/bin/python",
)
def insert(data: str):
    import nba_data_lake.nba.stats.db.commonallplayers as db
    import asyncio

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(db.CommonAllPlayers().insert(data))
    return result


with DAG(
    dag_id="nba_commonallplayers_async",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["nba"],
) as dag:
    delete()
    data = get()
    insert(data)
