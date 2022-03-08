import dask
import dask.array as da
import numpy as np
import pandas as pd
import prefect
import xarray as xr
from dask_kubernetes import KubeCluster, make_pod_spec
from prefect import Flow, Parameter, task
from prefect.executors import DaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret
from skimage import morphology

from oxeo.core.models.timeseries import merge_masks_all_constellations
from oxeo.core.models.waterbody import WaterBody
from oxeo.flows.utils import (
    data2gdf_task,
    fetch_water_list_task,
    get_waterbodies_task,
    parse_constellations_task,
    parse_water_list_task,
)
from oxeo.water.metrics import seg_area_all


def masker(data):
    print(f"load for {data.shape=}")
    arr = data.compute().data
    print("clean_frame for each slice")
    all_masks = [clean_frame(arr[i, ...]) for i in range(len(data))]
    print("stack")
    block = da.stack(all_masks, axis=0).compute()
    print("to xr")
    seg = xr.DataArray(block, dims=["revisits", "height", "width"])
    print("reduce")
    area = (seg > 0).sum(axis=(-2, -1))
    print("return")
    return area


def clean_frame(lab: np.ndarray) -> np.ndarray:
    lab = lab.astype(bool)
    lab = morphology.closing(lab, morphology.square(3))
    lab = morphology.remove_small_holes(lab, area_threshold=50, connectivity=2)
    lab = morphology.remove_small_objects(lab, min_size=50, connectivity=2)
    lab = morphology.label(lab, background=0, connectivity=2)
    masked = 1 * lab
    keepers = [val for val in np.unique(masked) if val != 0]
    fin = np.isin(lab, keepers)
    da_arr = da.from_array(fin)
    return da_arr


@task(log_stdout=True)
def long_func():
    a = xr.DataArray(da.ones((100, 5000, 5000)), dims=["time", "Y", "X"])
    dask.config.set(**{"array.slicing.split_large_chunks": False})
    a.groupby_bins("time", [20, 30]).map(masker)


@task(log_stdout=True)
def merge_to_timeseries(waterbodies: list[WaterBody]) -> pd.DataFrame:
    waterbody = waterbodies[0]
    logger = prefect.context.get("logger")
    logger.info(f"Merge {[(tp.tile.id, tp.constellation) for tp in waterbody.paths]}")
    timeseries_masks = merge_masks_all_constellations(waterbody=waterbody, mask="cnn")
    df = seg_area_all(timeseries_masks, waterbody, 1)
    return df


repo_name = "oxfordeo/oxeo-flows"
image = "eu.gcr.io/oxeo-main/oxeo-flows:latest"
prefect_secret_github_token = "GITHUB"

env = {"PREFECT__LOGGING__EXTRA_LOGGERS": '["oxeo.water"]'}


def cluster():
    cpu = 14
    memory = "56G"
    pod_spec = make_pod_spec(
        image=image,
        memory_request=memory,
        memory_limit=memory,
        cpu_request=cpu,
        cpu_limit=cpu,
        env=env,
    )
    return KubeCluster(
        n_workers=1,
        pod_template=pod_spec,
        scheduler_pod_template=make_pod_spec(image=image, env=env),
    )


executor = DaskExecutor(cluster_class=cluster)
storage = GitHub(
    repo=repo_name,
    path="oxeo/flows/dask_issue.py",
    access_token_secret=prefect_secret_github_token,
)
run_config = KubernetesRun(image=image, env=env)
with Flow(
    "dask_issue",
    executor=executor,
    storage=storage,
    run_config=run_config,
) as flow:
    # _ = long_func()

    postgis_password = PrefectSecret("POSTGIS_PASSWORD")

    water_list = Parameter(name="water_list", default=[-1931146])
    root_dir = Parameter(name="root_dir", default="gs://oxeo-water/chris-test")
    constellations = Parameter(name="constellations", default=["sentinel-2"])

    constellations = parse_constellations_task(constellations)
    water_list = parse_water_list_task(water_list, postgis_password)

    db_data = fetch_water_list_task(water_list=water_list, password=postgis_password)
    gdf = data2gdf_task(db_data)

    waterbodies = get_waterbodies_task(gdf, constellations, root_dir)
    _ = merge_to_timeseries(waterbodies=waterbodies)
