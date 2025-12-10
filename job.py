from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

def job_start(catalog, schema, file_name):
  return w.jobs.run_now(
    job_id="81258809834340",
    job_parameters = {"catalog":catalog, "schema": schema, "file_name":file_name}
    )
  
def job_status(run_id):
  run = w.jobs.get_run(run_id)
  return run.state.result_state