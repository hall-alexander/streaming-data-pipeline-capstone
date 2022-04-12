from _named_tuples import AreaDimensions, AffineCoefficients, Corners, Point
from typing import List
from math import radians, sin, cos, sqrt, atan
import numpy as np

SOUTH_CHINA_SEA_DIMENSIONS = AreaDimensions(corners=Corners(bottom_left=Point(x=123,y=7),bottom_right=Point(x=105,y=7),top_left=Point(x=123,y=23),top_right=Point(x=105,y=23)), height=16, width=1)

def get_affine_coefficients(area_dimensions:AreaDimensions) -> AffineCoefficients:
    '''
    Compute coefficients for affine transformation. Cartesian coordinates to lat/long coordinates
    Only need to compute this one time. Then use same coefficients for each coordinate conversion.
    area_dimensions: Contains the corners, width, and height of a geographic area.

    Returns AffineCoefficients: AffineCoefficients(a=A, b=B, c=C, d=D, e=E, f=F, delta_y=delta_y)
    '''
    corners=area_dimensions.corners

    delta_xc_2_0 = abs(corners.top_right.x - corners.bottom_right.x)
    delta_yc_2_0 = abs(corners.top_right.y - corners.bottom_right.y)
    delta_yp_2_0 = area_dimensions.height
    delta_yc_1_0 = abs(corners.bottom_left.y - corners.bottom_right.y)
    delta_xc_1_0 = abs(corners.bottom_left.x - corners.bottom_right.x)
    delta_xp_1_0 = area_dimensions.width

    theta = atan(abs(corners.bottom_right.y - corners.bottom_left.y) / abs(corners.bottom_right.x - corners.bottom_left.x)) #radians
    sx = sqrt(delta_xc_1_0 ** 2 + delta_yc_1_0 ** 2) / delta_xp_1_0 #scale factor
    sy = sqrt(delta_xc_2_0 ** 2 + delta_yc_2_0 ** 2) / delta_yp_2_0 #scale factor
    kx = delta_xc_2_0 / delta_yp_2_0
    ky = delta_yc_1_0 / delta_xp_1_0
    A = sx *((1 + kx * ky) * cos(theta) + ky * sin(theta))
    B = sx * (kx * cos(theta) + sin(theta))
    C = corners.bottom_left.x
    D = sy * (-(1 + kx * ky) * sin(theta) + ky * cos(theta))
    E = sy * (-kx * sin(theta) + cos(theta))
    F = corners.bottom_left.y

    delta_y = delta_yc_2_0
    return AffineCoefficients(a=A, b=B, c=C, d=D, e=E, f=F, delta_y=delta_y)

def geodesic_to_cartesian(point:List[float], affine_coeffs:AffineCoefficients) -> Point:
    '''
    Convert geodesic (longitude, latitude) coordinates to cartesian coordinates (column, row)
    affine_coeffs: list of coefficients for affine transformation. Computed with affine_coeffs function.
    point: location in (longitude, latitude)
    '''
    point = Point(x=point[0], y=point[1])

    mat_1 = np.array([[affine_coeffs.a, affine_coeffs.b], [affine_coeffs.d, affine_coeffs.e]])
    vec_1 = np.array([(point.x - affine_coeffs.c), (point.y - affine_coeffs.f - affine_coeffs.delta_y)])
    mat_1_inv = np.linalg.inv(mat_1)
    vec_2 = np.matmul(mat_1_inv, vec_1)
    vec_2 = vec_2.tolist()
    x_pixel = vec_2[0]
    y_pixel = -vec_2[1]
    #return Point(x=x_pixel, y=y_pixel)
    return [x_pixel, y_pixel]

def geodetic_to_geocentric(point:List[float]):
    """Return geocentric (Cartesian) Coordinates x, y, z corresponding to
    the geodetic coordinates given by latitude and longitude (in
    degrees) and height above ellipsoid. The ellipsoid must be
    specified by a pair (semi-major axis, reciprocal flattening).

    """
    ellipsoid = (6378137, 298.257223563)
    height = 124
    longitude = point[0]
    latitude = point[1]
    latitude = radians(latitude)
    longitude = radians(longitude)
    sin_latitude = sin(latitude)
    a, rf = ellipsoid           # semi-major axis, reciprocal flattening
    e2 = 1 - (1 - 1 / rf) ** 2  # eccentricity squared
    n = a / sqrt(1 - e2 * sin_latitude ** 2) # prime vertical radius
    r = (n + height) * cos(latitude)   # perpendicular distance from z axis
    x = r * cos(longitude)
    y = r * sin(longitude)
    z = (n * (1 - e2) + height) * sin_latitude
    return (x, y)