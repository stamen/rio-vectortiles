import click
import os
import json
import numpy as np
import mercantile
import sqlite3
import rasterio
from rasterio.warp import transform_bounds
from functools import partial
from random import shuffle
from multiprocessing import Pool
from rio_vectortiles import read_transform_tile, decompress_tile


@click.group("vectortiles")
@click.pass_context
def cli(ctx):
    pass


def get_maxzoom(w, s, e, n, rows, cols, extent):
    """Maxzoom estimation from bounding box, shape, and tile extent"""
    res = min((e - w) / cols, (n - s) / rows)
    log2 = np.log(2)
    C = 40075016.686
    return int(np.round(np.log(C / res) / log2 - np.log(extent) / log2))


def _extent_func(z, maxz=None, min_extent=None, max_extent=None):
    """Calculate extent from range and zooms"""
    return max([int(max_extent / 2 ** (maxz - z)), min_extent])


@cli.command("dump", short_help="Dump out tiles from an mbtiles")
@click.argument("mbtiles", type=click.Path(resolve_path=True))
@click.argument("output-directory", type=click.Path(resolve_path=True))
def dump_tiles(mbtiles, output_directory):
    """Dump decompressed tiles to disk"""
    sqlite3.connect(":memory:").close()
    reader = sqlite3.connect(mbtiles)
    cur = reader.cursor()
    for x, y, z, b in cur.execute(
        "SELECT tile_column, tile_row, zoom_level, tile_data from tiles"
    ):
        tiley = int(2**z) - y - 1
        with open(f"{output_directory}/{z}-{x}-{tiley}.mvt", "wb") as dst:
            dst.write(decompress_tile(b))


@cli.command("clump", short_help="Clump and index tiles from an mbtiles")
@click.argument("mbtiles", type=click.Path(resolve_path=True))
@click.argument("output-clump", type=click.Path(resolve_path=True))
@click.argument("output-index", type=click.Path(resolve_path=True))
def clump_tiles(mbtiles, output_clump, output_index):
    """Clump decompressed tiles + index"""
    sqlite3.connect(":memory:").close()
    reader = sqlite3.connect(mbtiles)
    cur = reader.cursor()
    tile_map = {}
    c = 0
    with open(output_clump, "wb") as dst:
        for x, y, z, b in cur.execute(
            "SELECT tile_column, tile_row, zoom_level, tile_data from tiles"
        ):
            s = len(b)
            dst.write(b)
            tiley = int(2**z) - y - 1
            tile_map[f"{z}/{x}/{tiley}"] = [c, c + s]
            c += s
    with open(output_index, "w") as dst:
        json.dump(tile_map, dst)


@cli.command("tile", short_help="Export a dataset to MBTiles.")
@click.argument("input_raster", type=click.Path(resolve_path=True))
@click.argument("output_mbtiles", type=click.Path(resolve_path=True))
@click.option(
    "--min-extent", default=256, type=int, help="The minimum vector tile extent to use"
)
@click.option(
    "--max-extent",
    default=512,
    type=int,
    help="The maximum vector tile extent to use (at max zoom)",
)
@click.option(
    "--zoom-adjust",
    default=0,
    type=int,
    help="Number of zoom levels to extend from pixel-derived maxzoom",
)
@click.option("--minzoom", default=0)
@click.option(
    "--interval", type=float, default=None, help="Data interval to vectorize on"
)
@click.option(
    "--bbox", type=str, default=None, help="Only generate tiles within this bbox"
)
@click.option("--dryrun", is_flag=True)
def vectortiles(
    input_raster,
    output_mbtiles,
    min_extent,
    max_extent,
    interval,
    zoom_adjust,
    bbox,
    minzoom,
    dryrun,
):
    """Raster-optimized vector tiler"""
    with rasterio.open(input_raster) as src:
        wm_bounds = transform_bounds(src.crs, "EPSG:3857", *src.bounds)
        maxzoom = get_maxzoom(*wm_bounds, *src.shape, max_extent) + zoom_adjust

        wgs_bounds = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
        if bbox is not None:
            wgs_bounds = json.loads(bbox)

        tiles = list(mercantile.tiles(*wgs_bounds, range(minzoom, maxzoom + 1)))

        extent_func = partial(
            _extent_func, maxz=maxzoom, min_extent=min_extent, max_extent=max_extent
        )

        extents = [extent_func(z) for z in range(minzoom, maxzoom + 1)]

        click.echo(
            f"Generating {len(tiles):,} tiles from zooms {minzoom}-{maxzoom} within {wgs_bounds}",
            err=True,
        )
        click.echo(f"Using internal extents of {extents}", err=True)
        if not dryrun:
            dst_profile = dict(
                driver="GTiff", count=1, dtype=src.meta["dtype"], crs="EPSG:3857"
            )

            if os.path.exists(output_mbtiles):
                os.unlink(output_mbtiles)

            sqlite3.connect(":memory:").close()
            writer = sqlite3.connect(output_mbtiles, isolation_level=None)

            cur = writer.cursor()

            cur.execute(
                "CREATE TABLE IF NOT EXISTS tiles "
                "(zoom_level integer, tile_column integer, "
                "tile_row integer, tile_data blob);"
            )
            cur.execute(
                "CREATE UNIQUE INDEX idx_zcr ON tiles (zoom_level, tile_column, tile_row);"
            )
            cur.execute("CREATE TABLE IF NOT EXISTS metadata (name text, value text);")

            cur.execute(
                "INSERT INTO metadata (name, value) VALUES (?, ?);",
                ("name", "rio-vectortiles"),
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
                (
                    "json",
                    json.dumps(
                        {
                            "vector_layers": [
                                {
                                    "id": "raster",
                                    "minzoom": 0,
                                    "maxzoom": maxzoom,
                                    "fields": {},
                                }
                            ]
                        }
                    ),
                ),
            )

            tiling_func = partial(
                read_transform_tile,
                src_path=input_raster,
                output_kwargs=dst_profile,
                extent_func=extent_func,
                interval=interval,
            )
            tile_sizes = {z: [] for z in range(minzoom, maxzoom + 1)}
            # shuffle the tiles to make a better guess as to tiling time
            shuffle(tiles)
            with click.progressbar(length=len(tiles), label="Tiling") as bar:
                with Pool() as pool:
                    for b, (x, y, z) in pool.imap_unordered(tiling_func, tiles):
                        tiley = int(2**z) - y - 1
                        cur.execute(
                            "INSERT OR REPLACE INTO tiles "
                            "(zoom_level, tile_column, tile_row, tile_data) "
                            "VALUES (?, ?, ?, ?);",
                            (z, x, tiley, sqlite3.Binary(b)),
                        )
                        tile_sizes[z].append(len(b))
                        bar.update(1)

            writer.commit()

            for z, t in tile_sizes.items():
                click.echo(
                    {
                        "zoom": z,
                        "min": min(t) / 1000,
                        "mean": np.mean(t) / 1000,
                        "max": max(t) / 1000,
                    }
                )
