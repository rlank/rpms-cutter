import rasterio

from rasterio.mask import mask
from rasterio.warp import transform_bounds

import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/dask-raster-worker.json'

def get_raster_bounds_crs(raster_path):
    """
    Extracts the bounding box, CRS, and transform information from a raster file.

    :param raster_path: Path to the raster file.
    :return: A tuple containing:
        - bounds (tuple): (min_x, min_y, max_x, max_y) representing the bounding box.
        - crs (str): Coordinate reference system (e.g., EPSG:4326).
        - transform (Affine): Affine transformation matrix (links pixel coordinates to geographic coordinates).
    """
    with rasterio.open(raster_path) as dataset:
        # Get bounding box (min_x, min_y, max_x, max_y)
        bounds = dataset.bounds
        # Get CRS (Coordinate Reference System)
        crs = dataset.crs.to_string()  # Convert CRS to string (e.g., 'EPSG:4326')
        # Get transform (links pixel coordinates to geographic coordinates)
        transform = dataset.transform

        return (bounds, crs, transform)


def load_raster_from_gcp(path):
    """
    Load a raster file from Google Cloud Platform (GCP) using Rasterio.
    This function attempts to open a raster file located at the specified path
    on GCP. It uses a Rasterio environment session to manage credentials and
    other configurations.
    Args:
        path (str): The path to the raster file on GCP.
    Returns:
        rasterio.io.DatasetReader: An object representing the opened raster file.
    Raises:
        rasterio.errors.RasterioIOError: If there is an error opening the raster file.
    """

    with rasterio.Env(session):
        try:
            src = rasterio.open(path, **read_options)
            return src
        except rasterio.errors.RasterioIOError as rio_er:
            # print(rio_er)
            raise rio_er


def clip_raster_to_bounds(bounds, crs, transform, year, output_path):
    """
    Clips a raster file to the specified bounding box and saves the result to a new file.
    Args:
        bounds (tuple): A tuple (min_x, min_y, max_x, max_y) representing the bounding box to clip to.
        crs (str): Coordinate Reference System of the input bounds.
        transform (Affine): Transform for the raster data.
        year (int): Year of the RPMS data.
        output_path (str): The path to save the clipped raster file.
    Returns:
        None
    """
    session = rasterio.session.GSSession()

    with rasterio.Env(session):
        # Calculate the bounds and transform for the clipped raster from the input bounds
        # and the clip bounds

        with rasterio.open(f"gs://fuelcast-public/rpms/{year}/rpms_{year}.tif") as src:    

            m_crs = rasterio.CRS.from_wkt(src.crs.to_wkt())
            t_bounds = transform_bounds(crs, m_crs, *bounds)

            out_image, out_transform = mask(src, [bounds], crop=True)
            out_meta = src.meta.copy()
            
            out_meta.update({
                'driver': 'GTiff',
                'height': out_image.shape[1],
                'width': out_image.shape[2],
                'transform': out_transform
            })

            # Write clipped data to output path
            with rasterio.open(output_path, 'w', **out_meta) as dest:
                dest.write(out_image)
            
