from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime
from kubernetes.client import models as k8s

with DAG(dag_id="pvc_write_test", start_date=datetime(2023, 1, 1), schedule_interval=None) as dag:

    volume = k8s.V1Volume(
        name="volume-rw",
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="vision-learning-non-cui"),
    )

    volume_mount = k8s.V1VolumeMount(
        mount_path="/workspace",
        name="volume-rw",
        read_only=False
    )

    task = KubernetesPodOperator(
        task_id="write-to-pvc",
        name="write-to-pvc",
        image="alpine",
        cmds=["sh", "-c"],
        arguments=["echo 'Hello from Airflow!' >> /workspace/temp.txt"],
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=True,
        get_logs=True,
    )
