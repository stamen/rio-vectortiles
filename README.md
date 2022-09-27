# rio-vectortiles

Rasterio plugin to vectorize and tiler raster data

## Usage
```
$ rio vectortiles --help
Usage: rio vectortiles [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  dump  Dump out tiles from an mbtiles
  tile  Export a dataset to MBTiles.
```

### `tile`
```
$ rio vectortiles tile --help
Usage: rio vectortiles tile [OPTIONS] INPUT_RASTER OUTPUT_MBTILES

  Raster-optimized vector tiler

Options:
  --min-extent INTEGER   The minimum vector tile extent to use
  --max-extent INTEGER   The maximum vector tile extent to use (at max zoom)
  --zoom-adjust INTEGER  Number of zoom levels to extend from pixel-derived
                         maxzoom
  --interval FLOAT       Data interval to vectorize on
  --help                 Show this message and exit.
  ```

### `dump`
```
rio vectortiles dump --help
Usage: rio vectortiles dump [OPTIONS] MBTILES OUTPUT_DIRECTORY

  Dump decompressed tiles to disk

Options:
  --help  Show this message and exit.
```