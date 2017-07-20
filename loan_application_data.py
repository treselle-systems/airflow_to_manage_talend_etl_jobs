import airflow
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
from airflow.utils.email import send_email

# Specify the shell script location for respective bash command
loan_analysis_command = "/home/ubuntu/airflow/jobs/loan_application_analysis_0.1/loan_application_analysis/loan_application_analysis_run.sh "
approved_command = "/home/ubuntu/airflow/jobs/approved/approved_0.1/approved/approved_run.sh "
withdrawn_command = "/home/ubuntu/airflow/jobs/withdrawn/withdrawn_0.1/withdrawn/withdrawn_run.sh "
denied_command = "/home/ubuntu/airflow/jobs/denied/denied_0.1/denied/denied_run.sh "
pl_app_command = "/home/ubuntu/airflow/jobs/approved/personal_0.1/personal/personal_run.sh "
hl_app_command = "/home/ubuntu/airflow/jobs/approved/home_0.1/home/home_run.sh "
al_app_command = "/home/ubuntu/airflow/jobs/approved/auto_0.1/auto/auto_run.sh "
cl_app_command = "/home/ubuntu/airflow/jobs/approved/credit_0.1/credit/credit_run.sh "
pl_dn_command = "/home/ubuntu/airflow/jobs/denied/personal_denied_0.1/personal_denied/personal_denied_run.sh "
hl_dn_command = "/home/ubuntu/airflow/jobs/denied/home_denied_0.1/home_denied/home_denied_run.sh "
al_dn_command = "/home/ubuntu/airflow/jobs/denied/auto_denied_0.1/auto_denied/auto_denied_run.sh "
cl_dn_command = "/home/ubuntu/airflow/jobs/denied/credit_denied_0.1/credit_denied/credit_denied_run.sh "
pl_wn_command = "/home/ubuntu/airflow/jobs/withdrawn/personal_withdrawn_0.1/personal_withdrawn/personal_withdrawn_run.sh "
hl_wn_command = "/home/ubuntu/airflow/jobs/withdrawn/home_withdrawn_0.1/home_withdrawn/home_withdrawn_run.sh "
al_wn_command = "/home/ubuntu/airflow/jobs/withdrawn/auto_withdrawn_0.1/auto_withdrawn/auto_withdrawn_run.sh "
cl_wn_command = "/home/ubuntu/airflow/jobs/withdrawn/credit_withdrawn_0.1/credit_withdrawn/credit_withdrawn_run.sh "

# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 3, 20),
    'schedule_interval':'45 09 * * *',
    'email': ['VALID_EMAIL_ID'],
    'email_on_failure': True,
    'email_on_success': True,
    'retries':3,
   
}

dag = DAG('loan_Application_Analysis', default_args=default_args)

# t1, t2, t3, etc are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='Loan_Application_Data',
    bash_command=loan_analysis_command,
    dag=dag)
	
t2 = BashOperator(
    task_id='Approved',
    bash_command=approved_command,
    dag=dag)
	
t3 = BashOperator(
    task_id='Denied',
    bash_command=denied_command,
    dag=dag)
	
t4 = BashOperator(
    task_id='Withdrawn',
    bash_command=withdrawn_command,
    dag=dag)
	
t5 = BashOperator(
    task_id='Approved_Personal',
    bash_command=pl_app_command,
    dag=dag)

t6 = BashOperator(
    task_id='Approved_Home',
    bash_command=hl_app_command,
    dag=dag)
	
t7 = BashOperator(
    task_id='Approved_Credit',
    bash_command=cl_app_command,
    dag=dag)
		
t8 = BashOperator(
    task_id='Approved_Auto',
    bash_command=al_app_command,
    dag=dag)
		
t9 = BashOperator(
    task_id='Denied_Personal',
    bash_command=pl_dn_command,
    dag=dag)

t10 = BashOperator(
    task_id='Denied_Home',
    bash_command=hl_dn_command,
    dag=dag)
	
t11 = BashOperator(
    task_id='Denied_Credit',
    bash_command=cl_dn_command,
    dag=dag)
		
t12 = BashOperator(
    task_id='Denied_Auto',
    bash_command=al_dn_command,
    dag=dag)
	
t13 = BashOperator(
    task_id='Withdrawn_Personal',
    bash_command=pl_wn_command,
    dag=dag)

t14 = BashOperator(
    task_id='Withdrawn_Home',
    bash_command=hl_wn_command,
    dag=dag)
	
t15 = BashOperator(
    task_id='Withdrawn_Credit',
    bash_command=cl_wn_command,
    dag=dag)
		
t16 = BashOperator(
    task_id='Withdrawn_Auto',
    bash_command=al_wn_command,
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)
t4.set_upstream(t1)
t5.set_upstream(t2)
t6.set_upstream(t2)
t7.set_upstream(t2)
t8.set_upstream(t2)
t9.set_upstream(t3)
t10.set_upstream(t3)
t11.set_upstream(t3)
t12.set_upstream(t3)
t13.set_upstream(t4)
t14.set_upstream(t4)
t15.set_upstream(t4)
t16.set_upstream(t4)
# This means that t2 will depend on t1
# running successfully to run
# It is equivalent to
# t1.set_downstream(t2)

