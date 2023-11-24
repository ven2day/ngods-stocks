import importlib
from dagster import job, op
from dagster_dbt import DbtCliResource

# Import the dbt module dynamically
dbt_module = importlib.import_module('dbt')

# Define dbt operations that use the dbt resource
@op(required_resource_keys={'dbt'})
def dbt_bronze_run_op(context, dependent_job=None):
    dbt_module._dbt_run(context)

@op(required_resource_keys={'dbt'})
def dbt_bronze_test_doc_sources_op(context, dependent_job=None):
    dbt_module._dbt_test(context)
    dbt_module._dbt_generate_docs(context)
    dbt_module._dbt_source_freshness(context)

@op(required_resource_keys={'dbt'})
def dbt_silver_run_op(context, dependent_job=None):
    dbt_module._dbt_run(context)

@op(required_resource_keys={'dbt'})
def dbt_silver_test_doc_sources_op(context, dependent_job=None):
    dbt_module._dbt_test(context)
    dbt_module._dbt_generate_docs(context)
    dbt_module._dbt_source_freshness(context)

@op(required_resource_keys={'dbt'})
def dbt_gold_run_op(context, dependent_job=None):
    dbt_module._dbt_run(context)

@op(required_resource_keys={'dbt'})
def dbt_gold_test_doc_sources_op(context, dependent_job=None):
    dbt_module._dbt_test(context)
    dbt_module._dbt_generate_docs(context)
    dbt_module._dbt_source_freshness(context)

# Define jobs that use the dbt resource
@job(resource_defs={'dbt': DbtCliResource(project_dir='project_dir', profiles_dir='profiles_dir')})
def dbt_bronze():
    dbt_bronze_test_doc_sources_op(dbt_bronze_run_op())

@job(resource_defs={'dbt': DbtCliResource(project_dir='project_dir', profiles_dir='profiles_dir')})
def dbt_silver():    
    dbt_silver_test_doc_sources_op(dbt_silver_run_op())

@job(resource_defs={'dbt': DbtCliResource(project_dir='project_dir', profiles_dir='profiles_dir')})
def dbt_gold():    
    dbt_gold_test_doc_sources_op(dbt_gold_run_op())

@job(resource_defs={'dbt': DbtCliResource(project_dir='project_dir', profiles_dir='profiles_dir')})
def dbt_all():    
    dbt_gold_test_doc_sources_op(
        dbt_gold_run_op(
            dbt_silver_test_doc_sources_op(
                dbt_silver_run_op(
                    dbt_bronze_test_doc_sources_op(
                        dbt_bronze_run_op()
                    )
                )
            )
        )
    )