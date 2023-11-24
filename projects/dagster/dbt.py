from dagster import job, op
from dagster_dbt import DbtCliResource

# Define dbt operations
def _dbt_run(context):
    # Get the project directory from the dbt resource
    pdir = context.resources.dbt.project_dir
    context.log.info(f"elt: executing dbt run with project_dir: '{pdir}'")
    if 'select' in context.op_config:
        s = context.op_config['select']        
        context.log.info(f"elt: executing dbt run with select: '{s}'")
        # Run dbt with the given selection
        context.resources.dbt.cli("run", select=s)
    else:
        # Run dbt without a specific selection
        context.resources.dbt.cli("run")

def _dbt_test(context):
    pdir = context.resources.dbt.project_dir
    context.log.info(f"elt: executing dbt test with project_dir: '{pdir}'")
    if 'select' in context.op_config:
        s = context.op_config['select']        
        context.log.info(f"elt: executing dbt run with select: '{s}'")
        context.resources.dbt.cli("test", select=s)
    else:
        context.resources.dbt.cli("test")

def _dbt_generate_docs(context):
    pdir = context.resources.dbt.project_dir
    context.log.info(f"elt: executing dbt docs generate with project_dir: '{pdir}'")
    if 'select' in context.op_config:
        s = context.op_config['select']        
        context.log.info(f"elt: executing dbt run with select: '{s}'")
        context.resources.dbt.cli("docs generate", select=s)
    else:
        context.resources.dbt.cli("docs generate")

def _dbt_source_freshness(context):
    pdir = context.resources.dbt.project_dir
    context.log.info(f"elt: executing dbt source freshness with project_dir: '{pdir}'")
    if 'select' in context.op_config:
        s = context.op_config['select']        
        context.log.info(f"elt: executing dbt run with select: '{s}'")
        context.resources.dbt.cli("source freshness", select=s)
    else:
        context.resources.dbt.cli("source freshness")

# Define dbt operations that use the dbt resource
@op(required_resource_keys={'dbt'})
def dbt_bronze_run_op(context, dependent_job=None):
    _dbt_run(context)

@op(required_resource_keys={'dbt'})
def dbt_bronze_test_doc_sources_op(context, dependent_job=None):
    _dbt_test(context)
    _dbt_generate_docs(context)
    _dbt_source_freshness(context)

@op(required_resource_keys={'dbt'})
def dbt_silver_run_op(context, dependent_job=None):
    _dbt_run(context)

@op(required_resource_keys={'dbt'})
def dbt_silver_test_doc_sources_op(context, dependent_job=None):
    _dbt_test(context)
    _dbt_generate_docs(context)
    _dbt_source_freshness(context)

@op(required_resource_keys={'dbt'})
def dbt_gold_run_op(context, dependent_job=None):
    _dbt_run(context)

@op(required_resource_keys={'dbt'})
def dbt_gold_test_doc_sources_op(context, dependent_job=None):
    _dbt_test(context)
    _dbt_generate_docs(context)
    _dbt_source_freshness(context)

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