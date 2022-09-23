from setuptools import setup, find_packages


with open("README.md") as f:
    readme = f.read()

setup(
    name="rio-vectortiles",
    version="0.1.0",
    description="Make vectortiles from rasters",
    long_description=readme,
    keywords="mapping, web mercator, tiles",
    author="Damon Burgett",
    author_email="damon@stamen.com",
    url="https://github.com/stamen/rio-vectortiles",
    license="BSD",
    packages=find_packages(exclude=["ez_setup", "examples", "tests"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=["click>=3.0", "cligj", "mercantile", "rasterio"],
    extras_require={
        "test": ["pytest"],
    },
    entry_points="""
      [rasterio.rio_plugins]
      vectortiles=rio_vectortiles.scripts.cli:vectortiles
      """,
)
