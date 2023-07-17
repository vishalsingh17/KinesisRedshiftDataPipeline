import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
import datetime

REDSHIFT_CONN_ID = 'redshift_conn'

default_args = {
    "owner": "redshiftdatapipelinepro",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "customer_orders_datapipeline_dynamic_batch_id",
    default_args=default_args,
    description="Runs data pipeline",
    schedule_interval=None,
    is_paused_upon_creation=False,
)

bash_task = BashOperator(task_id="run_bash_echo", bash_command="echo 1", dag=dag)

post_task = BashOperator(task_id="post_dbt", bash_command="echo 0", dag=dag)

batch_id = str(datetime.datetime.now().strftime("%Y%m%d%H%M"))
print("BATCH_ID = " + batch_id)

task_customer_landing_to_processing = BashOperator(
    task_id="customer_landing_to_processing",
    bash_command='aws s3 mv s3://redshiftdatapipelinepro/firehose/customers/landing/ s3://redshiftdatapipelinepro/firehose/customers/processing/{0}/ --recursive'.format(
        batch_id),
    dag=dag
)

task_customers_processing_to_processed = BashOperator(
    task_id="customer_processing_to_processed",
    bash_command='aws s3 mv s3://redshiftdatapipelinepro/firehose/customers/processing/{0}/ s3://redshiftdatapipelinepro/firehose/customers/processed/{0}/ --recursive'.format(
        batch_id),
    dag=dag
)

task_orders_landing_to_processing = BashOperator(
    task_id="orders_landing_to_processing",
    bash_command='aws s3 mv s3://redshiftdatapipelinepro/firehose/orders/landing/ s3://redshiftdatapipelinepro/firehose/orders/processing/{0}/ --recursive'.format(
        batch_id),
    dag=dag
)

task_orders_processing_to_processed = BashOperator(
    task_id="orders_processing_to_processed",
    bash_command='aws s3 mv s3://redshiftdatapipelinepro/firehose/orders/processing/{0}/ s3://redshiftdatapipelinepro/firehose/orders/processed/{0}/ --recursive'.format(
        batch_id),
    dag=dag
)

redshift_query_orders = """COPY PRO_SCHEMA.ORDERS_RAW (O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,O_COMMENT, BATCH_ID)
       FROM 's3://your-s3-bucket/orders_raw_stage_{0}'
       IAM_ROLE 'your-redshift-iam-role'
       FORMAT AS CSV
       IGNOREHEADER 1;""".format(batch_id)

redshift_query_customers = """COPY PRO_SCHEMA.CUSTOMER_RAW (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT, BATCH_ID)
       FROM 's3://your-s3-bucket/customer_raw_stage_{0}'
       IAM_ROLE 'your-redshift-iam-role'
       FORMAT AS CSV
       IGNOREHEADER 1;""".format(batch_id)

redshift_query_customer_orders_small_transformation = """INSERT INTO PRO_SCHEMA.ORDER_CUSTOMER_DATE_PRICE (CUSTOMER_NAME, ORDER_DATE, ORDER_TOTAL_PRICE, BATCH_ID)
       SELECT c.c_name AS customer_name, o.o_orderdate AS order_date, SUM(o.o_totalprice) AS order_total_price, c.batch_id
       FROM PRO_SCHEMA.ORDERS_RAW o
       JOIN PRO_SCHEMA.CUSTOMER_RAW c ON o.o_custkey = c.c_custkey AND o.batch_id = c.batch_id
       WHERE o.o_orderstatus = 'F'
       GROUP BY c.c_name, o.o_orderdate, c.batch_id
       ORDER BY o.o_orderdate;"""

redshift_orders_sql_str = RedshiftSQLOperator(
    task_id='redshift_raw_insert_order',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql=redshift_query_orders,
)

redshift_customers_sql_str = RedshiftSQLOperator(
    task_id='redshift_raw_insert_customers',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql=redshift_query_customers,
)

redshift_order_customers_small_transformation = RedshiftSQLOperator(
    task_id='redshift_order_customers_small_transformation',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql=redshift_query_customer_orders_small_transformation,
)

[task_orders_landing_to_processing >> redshift_orders_sql_str >> task_orders_processing_to_processed,
 task_customer_landing_to_processing >> redshift_customers_sql_str >> task_customers_processing_to_processed] >> redshift_order_customers_small_transformation >> post_task

