from dask_cloudprovider.gcp import GCPCluster

cloud_init = GCPCluster.get_cloud_init(
    projectid="oxeo-main",
    zone="europe-west4-a",
    network="dask",
    machine_type="n1-highmem-2",
    docker_image="eu.gcr.io/oxeo-main/oxeo-flows:latest",
)
print(cloud_init)
