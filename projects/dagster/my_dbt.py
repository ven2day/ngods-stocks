import importlib
import os
from dagster import job, op
from dagster_dbt import DbtCliResource

# Import the dbt module dynamically
dbt_module = importlib.import_module('my_dbt')

def _dbt_run(context):
    pdir = context.resources.my_dbt.project_dir
    context.log.info(f"elt: executing dbt run with project_dir: '{pdir}'")
    if 'select' in context.config:
        s = context.config['select']        
        context.log.info(f"elt: executing dbt run with select: '{s}'")
        context.resources.my_dbt.cli(["run", "--project-dir", pdir, "--select", s])
    else:
        context.resources.my_dbt.cli(["run", "--project-dir", pdir])

def _dbt_test(context):
    pdir = context.resources.my_dbt.project_dir
    context.log.info(f"elt: executing dbt test with project_dir: '{pdir}'")
    if 'select' in context.config:
        s = context.config['select']        
        context.log.info(f"elt: executing dbt run with select: '{s}'")
        context.resources.my_dbt.cli(["run", "--project-dir", pdir, "--select", s])
    else:
        context.resources.my_dbt.cli(["run", "--project-dir", pdir])

def _dbt_generate_docs(context):
    pdir = context.resources.my_dbt.project_dir
    context.log.info(f"elt: executing dbt docs generate with project_dir: '{pdir}'")
    if 'select' in context.config:
        s = context.config['select']        
        context.log.info(f"elt: executing dbt run with select: '{s}'")
        context.resources.my_dbt.cli(["run", "--project-dir", pdir, "--select", s])
    else:
        context.resources.my_dbt.cli(["run", "--project-dir", pdir])

def _dbt_source_freshness(context):
    pdir = context.resources.my_dbt.project_dir
    context.log.info(f"elt: executing dbt source freshness with project_dir: '{pdir}'")
    #context.resources.my_dbt.cli(["compile", "--project-dir", pdir])
    #manifest_path = os.path.join(pdir, "target", "manifest.json")
    if 'select' in context.config:
        s = context.config['select']        
        context.log.info(f"elt: executing dbt run with select: '{s}'")
        context.resources.my_dbt.cli(["run", "--project-dir", pdir, "--select", s])
    else:
        context.resources.my_dbt.cli(["run", "--project-dir", pdir])

# Define dbt operations that use the dbt resource
@op(required_resource_keys={'my_dbt'})
def dbt_bronze_run_op(context, dependent_job=None):
    dbt_module._dbt_run(context.resources.my_dbt)

@op(required_resource_keys={'my_dbt'})
def dbt_bronze_test_doc_sources_op(context, dependent_job=None):
    dbt_module._dbt_test(context.resources.my_dbt)
    dbt_module._dbt_generate_docs(context.resources.my_dbt)
    dbt_module._dbt_source_freshness(context.resources.my_dbt)

@op(required_resource_keys={'my_dbt'})
def dbt_silver_run_op(context, dependent_job=None):
    dbt_module._dbt_run(context.resources.my_dbt)

@op(required_resource_keys={'my_dbt'})
def dbt_silver_test_doc_sources_op(context, dependent_job=None):
    dbt_module._dbt_test(context.resources.my_dbt)
    dbt_module._dbt_generate_docs(context.resources.my_dbt)
    dbt_module._dbt_source_freshness(context.resources.my_dbt)

@op(required_resource_keys={'my_dbt'})
def dbt_gold_run_op(context, dependent_job=None):
    dbt_module._dbt_run(context.resources.my_dbt)

@op(required_resource_keys={'my_dbt'})
def dbt_gold_test_doc_sources_op(context, dependent_job=None):
    dbt_module._dbt_test(context.resources.my_dbt)
    dbt_module._dbt_generate_docs(context.resources.my_dbt)
    dbt_module._dbt_source_freshness(context.resources.my_dbt)

# Define jobs that use the dbt resource
@job(resource_defs={'my_dbt': DbtCliResource(project_dir='project_dir', profiles_dir='profiles_dir')})
def dbt_bronze():
    dbt_bronze_test_doc_sources_op(dbt_bronze_run_op())

@job(resource_defs={'my_dbt': DbtCliResource(project_dir='project_dir', profiles_dir='profiles_dir')})
def dbt_silver():    
    dbt_silver_test_doc_sources_op(dbt_silver_run_op())

@job(resource_defs={'my_dbt': DbtCliResource(project_dir='project_dir', profiles_dir='profiles_dir')})
def dbt_gold():    
    dbt_gold_test_doc_sources_op(dbt_gold_run_op())

@job(resource_defs={'my_dbt': DbtCliResource(project_dir='project_dir', profiles_dir='profiles_dir')})
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
