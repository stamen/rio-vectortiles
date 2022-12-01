"""rio_vectortiles"""
import mercantile
import rasterio

import gzip
from io import BytesIO

import numpy as np
from rasterio.warp import reproject
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds
from rasterio.enums import Resampling
import warnings
from PIL import Image

warnings.filterwarnings("ignore", category=rasterio.errors.NotGeoreferencedWarning)


def decompress_tile(tile_data):
    """Util to decompress data to bytes"""
    with BytesIO(tile_data) as src:
        with gzip.open(src, "rb") as gz:
            return gz.read()


def read_transform_tile(
    tile,
    src_path=None,
    output_kwargs=None,
    extent_func=None,
    interval=1,
    layer_name="raster",
    filters=[],
):
    """Warp to dimensions and vectorize

    Parameters
    ----------
    tile: mercantile.Tile
        the tile to create
    src_path: str
        path to the raster to transform
    output_kwargs: dict
        base creation options for the intermediate raster
    extent_func: func()
        a function needing a single parameter {z}
    interval: number
        interval to vectorize on
    layer_name: str
        name of the created raster layer
    interval: float
        interval to vectorize on

    Returns
    -------
    vector_tile: bytes
        gzipped-compressed vector tile
    tile: mercantile.Tile
        the passed-through tile object
    """
    xy_bounds = mercantile.xy_bounds(*tile)
    extent = extent_func(tile.z)
    dst_transform = from_bounds(*xy_bounds, extent, extent)

    dst_kwargs = {
        **output_kwargs,
        **{"transform": dst_transform, "width": extent, "height": extent},
    }

    with rasterio.open(src_path) as src:
        src_band = rasterio.band(src, bidx=1)

        with MemoryFile() as mem:
            with mem.open(**dst_kwargs) as dst:
                dst_band = rasterio.band(dst, bidx=1)
                reproject(src_band, dst_band, resampling=Resampling.mode)
                data = dst.read(1)
                A = data / 256
                B = data // 256
                C = B / 256
                D = B // 256

                blue = ((A - B) * 256).astype(np.uint8)
                green = ((C - D) * 256).astype(np.uint8)
                red = (((D / 256) - (D // 256)) * 256).astype(np.uint8)

                img = Image.fromarray(np.dstack([red, green, blue]))

    with BytesIO() as dst:
        img.save(dst, format="png")

        dst.seek(0)
        return dst.read(), tile
