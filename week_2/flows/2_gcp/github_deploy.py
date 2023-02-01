from prefect.deployments import Deployment
from etl_web_to_gcs_q4 import etl_parent_flow
from prefect.filesystems import GitHub

github_block = GitHub.load("zoomcamp")

github_dep = Deployment.build_from_flow(
    flow=etl_parent_flow, name="github-flow", infrastructure=github_block
)


if __name__ == "__main__":
    docker_dep.apply()
