import datetime
from airflow.operators.email_operator import EmailOperator
from airflow import DAG

def send_email(**context):
    
    task = context['ti'].task
    for parent_task in task.upstream_list:
        ti = TaskInstance(parent_task, args.execution_date)
        if ti.current_state() == "FAILURE":
            status = "Failed"
            break
        else:
            status = "Successful"
    
    
    subject = "Order {status}""
    body = f"""
        Hi {user}, <br>
        # Type your message here
         
        <br> Thank You. <br>
    """
    
    send_email(dag.default_args["email"], subject, body)

default_dag_args = {
    #'catchup_by_default': False,
    'start_date': datetime.datetime(2021, 5, 12),
    #'on_failure_callback': pass
}

with DAG(
        'Simple_email',
        default_args=default_dag_args) as dag:
        email = EmailOperator(
                task_id='send_email',
                to='u9m0c2t9p4h8k2w3@ibm-systems.slack.com',
                subject='Airflow Alert',
                html_content=""" <h3>Alert Email Test</h3> """,
                dag=dag
                )
        email

if __name__ == "__main__":
    dag.cli()