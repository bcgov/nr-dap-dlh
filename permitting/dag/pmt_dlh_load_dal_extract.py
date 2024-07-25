from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

ods_password = Variable.get("ODS_PASSWORD")
dlh_password = Variable.get("DLH_PASSWORD")

with DAG(
    start_date=datetime(2024, 2, 27),
    catchup=False,
    schedule=None,
    dag_id="pmt_dlh_load_dal_extract",
) as dag:
    start_task = DummyOperator(task_id="start_task")
    
    load_extract_permits = KubernetesPodOperator(
        task_id="task1_load_extract_permits",      
        image="ghcr.io/bcgov/nr-dap-dlh-pmt:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="task1_load_extract_permits",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev", "ConnectionType": "database"},
        env_vars={"ods_password": ods_password, "dlh_password": dlh_password},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"}),
        cmds=["dbt"], 
        arguments=["snapshot","--select","extract_permits","--profiles-dir","/usr/app/dbt/.dbt"]
        # arguments=["test","--profiles-dir","/usr/app/dbt/.dbt"]
    )
    
    # Set task dependencies
    start_task >> load_extract_permits
    
    
