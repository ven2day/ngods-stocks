from dagster import job, repository
from dagster_pyspark import pyspark_resource

from db import initialize_db, drop_tables_op, create_schemas_op, trino_resource
from download import download_yahoo_finance_files, download_yahoo_finance_files_op
from projects.dagster.my_dbt import dbt_bronze, dbt_silver, dbt_gold, dbt_all, dbt_bronze_run_op, dbt_silver_run_op, dbt_gold_run_op, dbt_bronze_test_doc_sources_op, dbt_silver_test_doc_sources_op, dbt_gold_test_doc_sources_op
from predict import predict_op

@job
def e2e():    
    from dagster_dbt import DbtCliResource  # Move the import here
    resource_defs={'my_dbt': DbtCliResource(project_dir='project_dir', profiles_dir='profiles_dir'), 'trino': trino_resource, 'pyspark': pyspark_resource}
    dbt_gold_test_doc_sources_op(
        dbt_gold_run_op(
            predict_op(
                dbt_silver_test_doc_sources_op(
                    dbt_silver_run_op(
                        dbt_bronze_test_doc_sources_op(
                            dbt_bronze_run_op(
                                download_yahoo_finance_files_op(
                                    drop_tables_op(
                                        create_schemas_op()
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )

@job(resource_defs={'pyspark': pyspark_resource})
def predict():
    predict_op()

@repository
def workspace():
    return [
        initialize_db, 
        download_yahoo_finance_files,
        dbt_bronze,
        dbt_silver,
        dbt_gold,
        dbt_all,
        e2e,
        predict
    ]