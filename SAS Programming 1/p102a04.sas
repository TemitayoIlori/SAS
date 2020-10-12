#Airflow imports
import airflow
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksRunNowOperator
from airflow.models import Variable
# General imports
from datetime import datetime, timedelta


yesterday = datetime.now()- timedelta(days=1)
REPORTDATE = yesterday.strftime("%Y-%m-%d")
reportdatearray = REPORTDATE.split("-")
reportdateyear = reportdatearray[0]
reportdatemonth = reportdatearray[1]
reportdateday = reportdatearray[2]

args = {
    'owner': 'tilori',
    'email': ['Teambi@cardinalcommerce.com'],
    'depends_on_past': False,
        'email_on_retry': False,
    'email_on_failure': True,
    'concurrency': 1,
    'start_date': datetime(2020,10,4)
}
dag = DAG(
    dag_id='dailyreport',
    default_args=args,
    catchup=False,
    schedule_interval="0 12 * * *"
    )

transactiondetailfileworldpay_job_id = 2627490

transactiondetailfileworldpay_notebook_task_params = {
    'reportdate': REPORTDATE,
        'outputFileSep': ",",
        'addHeaders': "true"
}

transactiondetailfileworldpay_notebook_task = DatabricksRunNowOperator(
    task_id='transactiondetailfileworldpay',
    job_id= transactiondetailfileworldpay_job_id,
    notebook_params=transactiondetailfileworldpay_notebook_task_params,
    dag=dag
)

worldpayDDCfile_script_task = BashOperator(
    task_id='worldpayDDCfile',
    bash_command='/cardinal/bi/prod/reports/reportscripts/worldpay/worldpayDDCfile.sh ' + reportdateyear + ' ' + reportdatemonth + ' ' + reportdateday,
    dag=dag
    )

worldpayDDCfile_script_task.set_upstream(transactiondetailfileworldpay_notebook_task)	
