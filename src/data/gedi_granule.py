"""Module for convenient objects to deal with GEDI data products"""
from __future__ import annotations

import pathlib
import re
from dataclasses import dataclass
from typing import Iterable, Union

import geopandas as gpd
import geopandas.array
import h5py
import numpy as np
import pandas as pd
import shapely.geometry

from src.constants import WGS84


@dataclass
class GediNameMetadata:
    """Data class container for metadata derived from GEDI file name conventions."""

    product: str
    year: str
    julian_day: str
    hour: str
    minute: str
    second: str
    orbit: str
    sub_orbit_granule: str
    ground_track: str
    positioning: str
    pge_version_number: str
    granule_production_version: str
    release_number: str


GEDI_SUBPATTERN = GediNameMetadata(
    product=r"\w+_\w",
    year=r"\d{4}",
    julian_day=r"\d{3}",
    hour=r"\d{2}",
    minute=r"\d{2}",
    second=r"\d{2}",
    orbit=r"O\d+",
    sub_orbit_granule=r"\d{2}",
    ground_track=r"T\d+",
    positioning=r"\d{2}",
    pge_version_number=r"\d{3}",
    granule_production_version=r"\d{2}",
    release_number=r"V\d+",
)


def _parse_gedi_granule_filename(gedi_filename: str) -> GediNameMetadata:
    """
    Parse a GEDI granule filename for the relevant metadata contained in the name.

    Args:
        gedi_filename (str): The filename to parse.

    Raises:
        ValueError: If the filename gedi_filename does not follow the GEDI conventions

    Returns:
        GediNameMetadata: The parsed metadata in a dataclass container
    """

    gedi_naming_pattern = re.compile(
        (
            f"({GEDI_SUBPATTERN.product})"
            f"_({GEDI_SUBPATTERN.year})"
            f"({GEDI_SUBPATTERN.julian_day})"
            f"({GEDI_SUBPATTERN.hour})"
            f"({GEDI_SUBPATTERN.minute})"
            f"({GEDI_SUBPATTERN.second})"
            f"_({GEDI_SUBPATTERN.orbit})"
            f"_({GEDI_SUBPATTERN.sub_orbit_granule})"
            f"_({GEDI_SUBPATTERN.ground_track})"
            f"_({GEDI_SUBPATTERN.positioning})"
            f"_({GEDI_SUBPATTERN.pge_version_number})"
            f"_({GEDI_SUBPATTERN.granule_production_version})"
            f"_({GEDI_SUBPATTERN.release_number})"
        )
    )
    parse_result = re.search(gedi_naming_pattern, gedi_filename)

    if parse_result is None:
        raise ValueError("Filename does not conform the the GEDI naming pattern.")
    return GediNameMetadata(*parse_result.groups())


class GediGranule(h5py.File):  # TODO  pylint: disable=missing-class-docstring
    def __init__(self, file_path: pathlib.Path):
        super().__init__(file_path, "r")
        self.file_path = file_path
        self.beam_names = [name for name in self.keys() if name.startswith("BEAM")]
        self._parsed_filename_metadata = None

    @property
    def version(self) -> str:
        return self["METADATA"]["DatasetIdentification"].attrs["VersionID"]

    @property
    def filename_metadata(self) -> GediNameMetadata:
        if self._parsed_filename_metadata is None:
            self._parsed_filename_metadata = _parse_gedi_granule_filename(self.filename)
        return self._parsed_filename_metadata

    @property
    def start_datetime(self) -> pd.Timestamp:
        return pd.to_datetime(
            (
                f"{self.filename_metadata.year}"
                f".{self.filename_metadata.julian_day}"
                f".{self.filename_metadata.hour}"
                f":{self.filename_metadata.minute}"
                f":{self.filename_metadata.second}"
            ),
            format="%Y.%j.%H:%M:%S",
        )

    @property
    def product(self) -> str:
        return self["METADATA"]["DatasetIdentification"].attrs["shortName"]

    @property
    def uuid(self) -> str:
        return self["METADATA"]["DatasetIdentification"].attrs["uuid"]

    @property
    def filename(self) -> str:
        return self["METADATA"]["DatasetIdentification"].attrs["fileName"]

    @property
    def abstract(self) -> str:
        return self["METADATA"]["DatasetIdentification"].attrs["abstract"]

    @property
    def n_beams(self) -> int:
        return len(self.beam_names)

    def beam(self, identifier: Union[str, int]) -> GediBeam:

        if isinstance(identifier, int):
            return self._beam_from_index(identifier)
        elif isinstance(identifier, str):
            return self._beam_from_name(identifier)
        else:
            raise ValueError("identifier must either be the beam index or beam name")

    def _beam_from_index(self, beam_index: int) -> GediBeam:
        if not 0 <= beam_index < self.n_beams:
            raise ValueError(f"Beam index must be between 0 and {self.n_beams-1}")

        beam_name = self.beam_names[beam_index]
        return self._beam_from_name(beam_name)

    def _beam_from_name(self, beam_name: str) -> GediBeam:
        if not beam_name in self.beam_names:
            raise ValueError(f"Beam name must be one of: {self.beam_names}")
        return GediBeam(granule=self, beam_name=beam_name)

    def iter_beams(self) -> Iterable[GediBeam]:
        for beam_index in range(self.n_beams):
            yield self._beam_from_index(beam_index)

    def list_beams(self) -> list[GediBeam]:
        return list(self.iter_beams())

    def __repr__(self) -> str:
        description = (
            "GEDI Granule:\n"
            f" Granule name: {self.filename}\n"
            f" Sub-granule:  {self.filename_metadata.sub_orbit_granule}\n"
            f" Product:      {self.product}\n"
            f" Release:      {self.filename_metadata.release_number}\n"
            f" No. beams:    {self.n_beams}\n"
            f" Start date:   {self.start_datetime.date()}\n"
            f" Start time:   {self.start_datetime.time()}\n"
            f" HDF object:   {super().__repr__()}"
        )
        return description


class GediBeam(h5py.Group):  # TODO  pylint: disable=missing-class-docstring
    def __init__(self, granule: GediGranule, beam_name: str):
        super().__init__(granule[beam_name].id)
        self.parent_granule = granule  # Reference to parent granule
        self._cached_data = None
        self._shot_lon_lat = None

    def list_datasets(self, top_level_only: bool = True) -> list[str]:
        if top_level_only:
            return list(self)
        else:
            # TODO
            raise NotImplementedError

    @property
    def name(self) -> str:
        return super().name[1:]

    @property
    def beam_type(self) -> str:
        return self.attrs["description"].split(" ")[0].lower()

    @property
    def quality(self) -> h5py.Dataset:
        return self["quality_flag"]

    @property
    def sensitivity(self) -> h5py.Dataset:
        return self["sensitivity"]

    @property
    def geolocation(self) -> h5py.Dataset:
        return self["geolocation"]

    @property
    def n_shots(self) -> int:
        return len(self["beam"])

    @property
    def shot_geolocations(self) -> geopandas.array.GeometryArray:
        """
        Return an array of shapely Point objects at the (lon, lat) of the lowest mode.

        Returns:
            geopandas.array.GeometryArray: A geometry array containing shapely Point
                objects at the (longitude, latitude) positions of the lowest detected
                mode (by the GEDI selected algorithm). Longitude, Latitude coordinates
                are given in the WGS84 coordinate reference system.
        """
        geolocations = np.array(
            [
                shapely.geometry.Point(lon, lat)
                for lon, lat in zip(self["lon_lowestmode"], self["lat_lowestmode"])
            ],
            dtype=shapely.geometry.Point,
        )

        return geopandas.array.GeometryArray(geolocations, crs=WGS84)

    @property
    def shot_lon_lat(self) -> list[tuple[float, float]]:
        if self._shot_lon_lat is None:
            self._shot_lon_lat = list(
                zip(self["lon_lowestmode"], self["lat_lowestmode"])
            )
        return self._shot_lon_lat

    @property
    def main_data(self) -> gpd.GeoDataFrame:
        if self._cached_data is None:
            data = self._get_main_data_dict
            geometry = self.shot_geolocations

            self._cached_data = gpd.GeoDataFrame(data, geometry=geometry, crs=WGS84)

        return self._cached_data

    @property
    def _get_main_data_dict(self) -> dict:
        """
        Return the main data for all shots in the beam as dictionary.

        Returns:
            dict: A dictionary containing the main data for all shots in the given
                beam of the granule.
        """
        # TODO: Adapt for Level 1B
        data = {
            "granule_name": [self.parent_granule.filename] * self.n_shots,
            "shot_number": self["shot_number"][:],
            "beam_type": [self.beam_type] * self.n_shots,
            "beam_name": [self.name] * self.n_shots,
            "delta_time": self["delta_time"][:],
            "sensitivity": self["sensitivity"][:],
            "quality_flag": self["quality_flag"][:],
            "solar_elevation": self["solar_elevation"][:],
            "solar_azimuth": self["solar_elevation"][:],
            "energy_total": self["energy_total"][:],
            "selected_algorithm": self["selected_algorithm"][:],
            "selected_mode": self["selected_mode"][:],
            "lon_lowestmode": self["lon_lowestmode"][:],
            "longitude_bin0_error": self["longitude_bin0_error"][:],
            "lat_lowestmode": self["lat_lowestmode"][:],
            "latitude_bin0_error": self["latitude_bin0_error"][:],
            "elev_lowestmode": self["elev_lowestmode"][:],
            "elevation_bin0_error": self["elevation_bin0_error"][:],
            "lon_highestreturn": self["lon_highestreturn"][:],
            "lat_highestreturn": self["lat_highestreturn"][:],
            "elev_highestreturn": self["elev_highestreturn"][:],
        } | {f"rh{i}": self["rh"][:, i] for i in range(101)}
        return data

    def __repr__(self) -> str:
        description = (
            "GEDI Beam object:\n"
            f" Beam name:  {self.name}\n"
            f" Beam type:  {self.attrs['description']}\n"
            f" Shots:      {self.n_shots}\n"
            f" HDF object: {super().__repr__()}"
        )
        return description

    def intersect(self, geometry):
        raise NotImplementedError
