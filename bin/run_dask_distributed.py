import os

from dask_cloudprovider.gcp import GCPCluster
import dask.config

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "oxeo-main-prefect.json"

dask.config.set(
    **{
        "cloudprovider.gcp.projectid": "oxeo-main",
        "public_ingress": True,
    }
)

with GCPCluster(
    zone="europe-west4-a",
    machine_type="n1-highmem-2",
    n_workers=1,
    docker_image="eu.gcr.io/oxeo-main/oxeo-flows:latest",
) as cluster:
    print(cluster)
    input("waiting")
    print("done")
