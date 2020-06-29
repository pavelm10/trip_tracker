import numpy as np


def geodesic_distance(lat1, lat2, lon1, lon2):
    """
    Calculates geodesic distance of two points (P1, P2) described by lat, lon pairs
    using Vincenty's algorithm of the inverse formula.
    Implementation based on: http://www.5thandpenn.com/GeoMaps/GMapsExamples/distanceComplete2.html
    Vincenty algorithm: https://www.ngs.noaa.gov/PUBS_LIB/inverse.pdf
    :param lat1: latitude [rad] of point P1
    :param lat2: latitude [rad] of point P2
    :param lon1: longitude [rad] of point P1
    :param lon2: longitude [rad] of point P2
    :return: geodesic distance [m] of points P1 and P2
    """
    if lon1 == lon2 and lat1 == lat2:
        return 0

    a = 6378137
    b = 6356752.3142
    f = 1 / 298.257223563
    eps = 1e-5
    precision = 1e-9

    lon_delta = np.abs(lon1 - lon2)
    u1 = np.arctan((1 - f) * np.tan(lat1))
    u2 = np.arctan((1 - f) * np.tan(lat2))
    lam = lon_delta
    lam_hat = 2 * np.pi

    while np.abs(lam - lam_hat) > precision:
        sin_lam = np.sin(lam)
        cos_lam = np.cos(lam)
        sin_u1 = np.sin(u1)
        cos_u1 = np.cos(u1)
        sin_u2 = np.sin(u2)
        cos_u2 = np.cos(u2)
        cos_u1_u2 = cos_u1 * cos_u2
        sin_u1_u2 = sin_u1 * sin_u2

        sin_sigma = np.sqrt((cos_u2 * sin_lam)**2 + (cos_u1 * sin_u2 - sin_u1 * cos_u2 * cos_lam)**2)
        cos_sigma = sin_u1_u2 + cos_u1_u2 * cos_lam
        sigma = np.arctan2(sin_sigma, cos_sigma)

        sin_alpha = cos_u1_u2 * sin_lam / sin_sigma
        cos_alpha_sq = 1 - sin_alpha**2

        cos2sigma_m = cos_sigma - 2 * sin_u1_u2 / cos_alpha_sq
        if cos_alpha_sq < eps:
            cos2sigma_m = 0

        c = f / (16 * cos_alpha_sq * (4 + f * (4 - 3 * cos_alpha_sq)))
        lam_hat = lam
        lam = lon_delta + (1 - c) * f * sin_alpha * (sigma + c * sin_sigma * (cos2sigma_m + c * cos_sigma * (-1 + 2 * cos2sigma_m)))

    u_sq = cos_alpha_sq * (a**2 - b**2) / b**2
    a_cor = 1 + (u_sq / 16384) * (4096 + u_sq * (-768 + u_sq * (320 - 175 * u_sq)))
    b_cor = (u_sq / 1024) * (256 + u_sq * (-128 + u_sq * (74 - 47 * u_sq)))
    sigma_delta = b_cor * sin_sigma * (cos2sigma_m + b_cor * 0.25 * (cos_sigma * (-1 + 2 * cos2sigma_m**2) - b_cor * cos2sigma_m * (-3 + 4 * sin_sigma**2) * (-3 + 4 * cos2sigma_m**2) / 6))
    distance = b * a_cor * (sigma - sigma_delta)
    return distance


def calculate_distance(pt1, pt2):
    dist = geodesic_distance(np.deg2rad(pt1['lat']),
                             np.deg2rad(pt2['lat']),
                             np.deg2rad(pt1['lon']),
                             np.deg2rad(pt2['lon']))
    return dist


if __name__ == "__main__":
    expected = 13308.3511
    lt1, lo1, lt2, lo2 = 49.0, 14.0, 49.1, 14.1
    point1 = {'lat': lt1, 'lon': lo1}
    point2 = {'lat': lt2, 'lon': lo2}

    out = calculate_distance(point1, point2)
    np.testing.assert_approx_equal(out, expected, 9)
