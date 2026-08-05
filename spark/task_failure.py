def task_failure_alert(context):
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    
    print(f"""
    DAG Failed!
    DAG: {dag_id}
    Task: {task_instance.task_id}
    Execution Time: {context['execution_date']}
    """)