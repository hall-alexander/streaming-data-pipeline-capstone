from typing import NamedTuple, List

class Point(NamedTuple):
    """Point class. x is longitude coordinate and y is latitude coordinate."""
    x: float
    y: float

class Corners(NamedTuple):
    """Corners of raster"""
    bottom_left: Point
    bottom_right: Point
    top_left: Point
    top_right: Point

class AreaDimensions(NamedTuple):
    """Contains the height (num of pixel rows), the width (num of pixel cols), and the corners of the input raster."""
    corners: List[Corners]
    height: int=0
    width: int=0

class AffineCoefficients(NamedTuple):
    a: float
    b: float
    c: float
    d: float
    e: float
    f: float
    delta_y: float