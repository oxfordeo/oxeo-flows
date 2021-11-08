from typing import List
from typing import Union
from pathlib import Path

import prefect
from prefect import task, Flow, Parameter
from prefect.client import Client
import shapely
import pystac

import satextractor
from satextractor.models import Tile, ExtractionTask


@task
def get_geometry(
    lake: int,
) -> Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon]:
    logger = prefect.context.get("logger")
    logger.info("Getting lake AOI geometry")
    # connect to PostGIS
    # convert lake id to geometry
    # and return it
    ...


@task
def build(
    project: str,
    gcp_region: str,
    storage_root: str,
    credentials: Path,
    user_id: str,
) -> None:
    logger = prefect.context.get("logger")
    logger.info("Building Google Cloud resources")
    satextractor.builder.gcpbuilder.build_gcp(
        project=project,
        region=gcp_region,
        storage_root=storage_root,
        credentials=credentials,
        user_id=user_id,
    )


@task
def stac(
    credentials: Path,
    start_date: str,
    end_date: str,
    constellations: str,
    region: Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon],
) -> pystac.ItemCollection:
    logger = prefect.context.get("logger")
    logger.info("Converting data to STAC")
    item_collection = satextractor.stac.gcp_region_to_item_collection(
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
    region: Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon],
) -> List[Tile]:
    logger = prefect.context.get("logger")
    logger.info("Creating tiles")
    tiles = satextractor.tiler.split_region_in_utm_tiles(
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
) -> List[ExtractionTask]:
    logger = prefect.context.get("logger")
    logger.info("Create extraction tasks")
    extraction_tasks = satextractor.scheduler.create_tasks_by_splits(
        tiles=tiles,
        split_m=100000000,
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
    tiles: List[Tile],
    extraction_tasks: List[ExtractionTask],
) -> None:
    logger = prefect.context.get("logger")
    logger.info("Prepare Cloud Storage bucket")
    satextractor.preparer.gcp_preparer.gcp_prepare_archive(
        credentials=credentials,
        tasks=extraction_tasks,
        tiles=tiles,
        constellations=constellations,
        storage_root=storage_path,
        patch_size=bbox_size,
        chunk_size=1000,
        n_jobs=-1,
        verbose=0,
    )


@task
def deployer(
    project: str,
    user_id: str,
    credentials: Path,
    storage_path: str,
    extraction_tasks: List[ExtractionTask],
) -> None:
    logger = prefect.context.get("logger")
    logger.info("Deploy tasks to Cloud RUn")

    topic = f"projects/{project}/topics/{'-'.join([user_id, 'stacextractor'])}"
    satextractor.deployer.deploy_tasks(
        credentials=credentials,
        extraction_tasks=extraction_tasks,
        storage_path=storage_path,
        chunk_size=1000,
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
        lake_id = Parameter(name="lake_id", required=True)

        credentials = Parameter(name="", default="../token.json")
        project = Parameter(name="project", default="oxeo-main")
        gcp_region = Parameter(name="gcp_region", default="europe-west4")
        user_id = Parameter(name="", default="oxeo")
        storage_root = Parameter(name="", default="oxeo-water")
        storage_path = f"gs://{storage_root}/{lake_id}"

        start_date = Parameter(name="", default="2020-01-01")
        end_date = Parameter(name="", default="2020-02-01")
        constellations = Parameter(
            name="", default=["sentinel-2", "landsat-5", "landsat-7", "landsat-8"]
        )
        bbox_size = Parameter(name="", default=10000)

        # rename the Flow run to reflect the parameters
        rename_flow_run(lake_id)

        # run the flow
        aoi = get_geometry(lake_id)
        build(project, gcp_region, storage_root, credentials, user_id)
        item_collection = stac(credentials, start_date, end_date, constellations, aoi)
        tiles = tiler(bbox_size, aoi)
        extraction_tasks = scheduler(constellations, tiles, item_collection)
        preparer(
            credentials,
            constellations,
            storage_path,
            bbox_size,
            tiles,
            extraction_tasks,
        )
        deployer(project, user_id, credentials, storage_path, extraction_tasks)

    return flow


if __name__ == "__main__":
    flow = create_flow()
    flow.register(project_name="test")
    # flow.run()
    # flow.visualize()
