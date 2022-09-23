import click
import os
import json
import numpy as np
import mercantile
import sqlite3
import rasterio
from rasterio.warp import transform_bounds
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from rio_vectortiles import read_transform_tile

def get_maxzoom(w, s, e, n, rows, cols, extent):
    """Cheap maxzoom estimation"""
    res = min((e - w) / cols, (n - s) / rows)
    
    return int(np.round(np.log(40075016.686 / res) / np.log(2) - np.log(extent) / np.log(2)))


def _extent_func(z, maxz=None, min_extent=None, max_extent=None):
    """Calculate extent from range and zooms
    """
    return max([int(max_extent / 2 ** (maxz - z)), min_extent])


@click.command(short_help="Export a dataset to MBTiles.")
@click.argument("input_raster", type=click.Path(resolve_path=True))
@click.argument("output_mbtiles", type=click.Path(resolve_path=True))
@click.option("--min-extent", default=256, type=int)
@click.option("--max-extent", default=512)
@click.pass_context
def vectortiles(ctx, input_raster, output_mbtiles, min_extent, max_extent):
    with rasterio.open(input_raster) as src:
        wm_bounds = transform_bounds(src.crs, "EPSG:3857", *src.bounds)
        maxzoom = get_maxzoom(*wm_bounds, *src.shape, max_extent)

        wgs_bounds = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
        tiles =list(mercantile.tiles(*wgs_bounds, range(maxzoom + 1)))
    
        dst_profile = dict(
            driver="GTiff",
            count=1,
            dtype=src.meta["dtype"],
            crs="EPSG:3857"
        )

        if os.path.exists(output_mbtiles):
            os.unlink(output_mbtiles)

        sqlite3.connect(":memory:").close()
        writer = sqlite3.connect(output_mbtiles, isolation_level=None)
        writer.execute('pragma journal_mode=wal;')

        cur = writer.cursor()

        cur.execute(
            "CREATE TABLE IF NOT EXISTS tiles "
            "(zoom_level integer, tile_column integer, "
            "tile_row integer, tile_data blob);"
        )
        cur.execute(
            "CREATE UNIQUE INDEX idx_zcr ON tiles (zoom_level, tile_column, tile_row);"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS metadata (name text, value text);"
        )

        cur.execute(
            "INSERT INTO metadata (name, value) VALUES (?, ?);", ("name", "rio-vectortiles")
        )
        cur.execute(
            "INSERT INTO metadata (name, value) VALUES (?, ?);",
            ("type", "pbf"),
        )
        cur.execute(
            "INSERT INTO metadata (name, value) VALUES (?, ?);",
            ("version", "1.1"),
        )
        cur.execute(
            "INSERT INTO metadata (name, value) VALUES (?, ?);",
            ("description", f"{input_raster}"),
        )

        cur.execute(
            "INSERT INTO metadata (name, value) VALUES (?, ?);",
            ("json", json.dumps({"vector_layers": [{"id": "raster", "minzoom": 0, "maxzoom": maxzoom, "fields": {"v": "String"}}]})))

        extent_func = partial(_extent_func, maxz=maxzoom, min_extent=min_extent, max_extent=max_extent)
        tiling_func = partial(read_transform_tile, src_path=input_raster, output_kwargs=dst_profile, extent_func=extent_func)

        with ThreadPoolExecutor() as pool:
            for b, (x, y, z) in pool.map(
                tiling_func, tiles):
                tiley = int(2 ** z) - y - 1
                cur.execute(
                        "INSERT OR REPLACE INTO tiles "
                        "(zoom_level, tile_column, tile_row, tile_data) "
                        "VALUES (?, ?, ?, ?);",
                        (z, x, tiley, sqlite3.Binary(b)),
                )
        writer.commit()