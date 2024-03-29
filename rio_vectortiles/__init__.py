"""rio_vectortiles"""
import mercantile
from shapely import geometry
import rasterio
from vtzero.tile import Tile, Layer, Polygon
import gzip
from io import BytesIO
from affine import Affine
import numpy as np
from rasterio.warp import reproject
from rasterio.io import MemoryFile
from rasterio.features import shapes, sieve
from rasterio.transform import from_bounds
from rasterio.enums import Resampling
import warnings
from itertools import groupby

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
        vtile = Tile()

        layer = Layer(vtile, layer_name.encode(), extent=extent)
        sieve_value = 2
        with MemoryFile() as mem:
            with mem.open(**dst_kwargs) as dst:
                dst_band = rasterio.band(dst, bidx=1)
                reproject(src_band, dst_band, resampling=Resampling.mode)
                dst.transform = Affine.identity()
                if interval is None and sieve_value:
                    data = sieve(dst_band, sieve_value)
                    vectorizer = shapes(data)
                elif interval is not None:
                    data = dst.read(1)
                    for f in filters:
                        data = f(data)
                    data = (data // interval * interval).astype(np.int32)
                    vectorizer = shapes(data)
                else:
                    vectorizer = shapes(dst_band)

                grouped_vectors = groupby(
                    sorted(vectorizer, key=lambda x: x[1]), key=lambda x: x[1]
                )

                for v, geoms in grouped_vectors:
                    feature = Polygon(layer)
                    feature.set_id(v)
                    geoms = geometry.MultiPolygon(
                        [geometry.shape(g) for g, _ in geoms]
                    ).buffer(0)
                    if geoms.geom_type == "Polygon":
                        iter_polys = [geoms]
                    else:
                        iter_polys = geoms.geoms
                    for geom in iter_polys:
                        feature.add_ring(len(geom.exterior.coords))
                        for coord in geom.exterior.coords:
                            feature.set_point(*coord)
                        for part in geom.interiors:
                            feature.add_ring(len(part.coords))
                            for coord in part.coords:
                                feature.set_point(*coord)
                    # feature.add_property(b"v", f"{int(v)}".encode())
                    feature.commit()

    with BytesIO() as dst:
        with gzip.open(dst, mode="wb") as gz:
            gz.write(vtile.serialize())
        dst.seek(0)
        return dst.read(), tile
