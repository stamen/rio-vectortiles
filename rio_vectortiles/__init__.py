"""rio_vectortiles"""
import mercantile
import rasterio

from vtzero.tile import Tile, Layer, Polygon
import gzip
from io import BytesIO

from rasterio.warp import reproject
from rasterio.io import MemoryFile
from rasterio.features import shapes
from rasterio.transform import from_bounds


def read_transform_tile(
    tile,
    src_path=None,
    output_kwargs=None,
    extent_func=None,
    interval=1,
    layer_name="raster",
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

        with MemoryFile() as mem:
            with mem.open(**dst_kwargs) as dst:
                dst_band = rasterio.band(dst, bidx=1)

                reproject(src_band, dst_band)

                for s, v in shapes(dst_band):
                    feature = Polygon(layer)
                    for ring in s["coordinates"]:
                        feature.add_ring(len(ring))
                        for x, y in ring:
                            feature.set_point(x * 8, y * 8)
                        feature.close_ring()
                    feature.add_property(b"v", f"{v}".encode())
                    feature.commit()

    with BytesIO() as dst:
        with gzip.open(dst, mode="wb") as gz:
            gz.write(vtile.serialize())
        dst.seek(0)
        return dst.read(), tile
