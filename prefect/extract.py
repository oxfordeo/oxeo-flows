from typing import List
from typing import Union
from pathlib import Path
from uuid import uuid4
import subprocess

import prefect
from prefect import task, Flow, Parameter
from prefect.client import Client
from shapely.geometry import Polygon, MultiPolygon
import pystac
import typer
import geopandas as gpd

# from satextractor.builder.gcp_builder import build_gcp
from satextractor.stac import gcp_region_to_item_collection
from satextractor.tiler import split_region_in_utm_tiles
from satextractor.scheduler import create_tasks_by_splits
from satextractor.preparer.gcp_preparer import gcp_prepare_archive
from satextractor.deployer import deploy_tasks
from satextractor.models import Tile, ExtractionTask


@task
def get_geometry(
    aoi_path: Path,
) -> Union[Polygon, MultiPolygon]:
    logger = prefect.context.get("logger")
    logger.info("Getting lake AOI geometry")
    return gpd.read_file(aoi_path).unary_union


@task
def get_storage_path(
    storage_root: str,
    aoi_id: str,
) -> str:
    return f"gs://{storage_root}/{aoi_id}"


@task
def build(
    project: str,
    gcp_region: str,
    storage_root: str,
    credentials: Path,
    user_id: str,
) -> None:
    logger = prefect.context.get("logger")

    # logger.info("Building Google Cloud resources")
    # build_gcp(
    # credentials=credentials,
    # project=project,
    # region=gcp_region,
    # storage_root=storage_root,
    # user_id=user_id,
    # )

    logger.info("Checking that Google Cloud Run and PubSub resources exist")
    cmd = [
        "gcloud",
        "run",
        "services",
        "describe",
        f"{user_id}-stacextractor",
        f"--region={gcp_region}",
        "--platform=managed",
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    assert p.stderr == ""

    cmd = [
        "gcloud",
        "pubsub",
        "subscriptions",
        "describe",
        f"{user_id}-stacextractor",
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    assert p.stderr == ""

    return True


@task
def stac(
    credentials: Path,
    start_date: str,
    end_date: str,
    constellations: str,
    region: Union[Polygon, MultiPolygon],
) -> pystac.ItemCollection:
    logger = prefect.context.get("logger")
    logger.info("Converting data to STAC")
    item_collection = gcp_region_to_item_collection(
        credentials=credentials,
        region=region,
        start_date=start_date,
        end_date=end_date,
        constellations=constellations,
    )
    return item_collection


@task
def tiler(
    bbox_size: int,
    region: Union[Polygon, MultiPolygon],
) -> List[Tile]:
    logger = prefect.context.get("logger")
    logger.info("Creating tiles")
    tiles = split_region_in_utm_tiles(
        region=region,
        # crs=,
        bbox_size=(bbox_size, bbox_size),
    )
    return tiles


@task
def scheduler(
    constellations: List[str],
    tiles: List[Tile],
    item_collection: pystac.ItemCollection,
    split_m: int,
) -> List[ExtractionTask]:
    logger = prefect.context.get("logger")
    logger.info("Create extraction tasks")
    extraction_tasks = create_tasks_by_splits(
        tiles=tiles,
        split_m=split_m,
        item_collection=item_collection,
        constellations=constellations,
        bands=None,
        interval=1,
        n_jobs=-1,
        verbose=0,
    )
    return extraction_tasks


@task
def preparer(
    credentials: Path,
    constellations: List[str],
    storage_path: str,
    bbox_size: int,
    chunk_size: int,
    tiles: List[Tile],
    extraction_tasks: List[ExtractionTask],
) -> None:
    logger = prefect.context.get("logger")
    logger.info("Prepare Cloud Storage bucket")
    gcp_prepare_archive(
        credentials=credentials,
        tasks=extraction_tasks,
        tiles=tiles,
        constellations=constellations,
        storage_root=storage_path,
        patch_size=bbox_size,
        chunk_size=chunk_size,
        n_jobs=-1,
        verbose=0,
    )


@task
def deployer(
    project: str,
    user_id: str,
    credentials: Path,
    storage_path: str,
    chunk_size: int,
    extraction_tasks: List[ExtractionTask],
) -> None:
    logger = prefect.context.get("logger")
    logger.info("Deploy tasks to Cloud RUn")

    topic = f"projects/{project}/topics/{'-'.join([user_id, 'stacextractor'])}"
    deploy_tasks(
        credentials=credentials,
        extraction_tasks=extraction_tasks,
        storage_path=storage_path,
        chunk_size=chunk_size,
        topic=topic,
    )


@task
def rename_flow_run(
    lake: int,
) -> None:
    logger = prefect.context.get("logger")
    new_name = f"run_{lake}"
    logger.info(f"Rename the Flow Run to {new_name}")
    Client().set_flow_run_name(prefect.context.get("flow_run_id"), new_name)


def create_flow():
    with Flow("extract") as flow:
        # parameters
        # aoi_id = Parameter(name="aoi_id", required=True)
        aoi_id = str(uuid4())[:8]
        # geojson = Parameter(name="geojson", require=False)
        aoi_path = Parameter(name="aoi_path")

        credentials = Parameter(name="credentials", default="../token.json")
        project = Parameter(name="project", default="oxeo-main")
        gcp_region = Parameter(name="gcp_region", default="europe-west4")
        user_id = Parameter(name="user_id", default="oxeo")
        storage_root = Parameter(name="storage_root", default="oxeo-water")

        start_date = Parameter(name="start_date", default="2020-01-01")
        end_date = Parameter(name="end_date", default="2020-02-01")
        constellations = Parameter(
            name="constellations",
            default=["sentinel-2", "landsat-5", "landsat-7", "landsat-8"],
        )

        bbox_size = Parameter(name="bbox_size", default=10000)
        split_m = Parameter(name="split_m", default=100000)
        chunk_size = Parameter(name="chunk_size", default=1000)

        # rename the Flow run to reflect the parameters
        rename_flow_run(aoi_id)

        # run the flow
        aoi = get_geometry(aoi_path)
        storage_path = get_storage_path(storage_root, aoi_id)
        built = build(project, gcp_region, storage_root, credentials, user_id)
        item_collection = stac(credentials, start_date, end_date, constellations, aoi)
        tiles = tiler(bbox_size, aoi)
        extraction_tasks = scheduler(constellations, tiles, item_collection, split_m)
        prepped = preparer(
            credentials,
            constellations,
            storage_path,
            bbox_size,
            chunk_size,
            tiles,
            extraction_tasks,
        )
        deployer(
            project,
            user_id,
            credentials,
            storage_path,
            chunk_size,
            extraction_tasks,
            upstream_tasks=[built, prepped],
        )

    return flow


app = typer.Typer()


@app.command()
def register(project_name: str):
    flow = create_flow()
    flow.register(project_name=project_name)


@app.command()
def run(aoi_path: Path):
    flow = create_flow()
    flow.run(aoi_path=aoi_path)


@app.command()
def visualize():
    flow = create_flow()
    flow.visualize()


if __name__ == "__main__":
    app()
