import numpy as np
import xarray as xr

def run_preprocess(dataset):
    # Convert dimension names to lat/lon
    l3m_data = dataset.data_vars['l3m_data']
    dims = l3m_data.sizes
    dim_translations = {}

    for name, size in dims.items():
        if size == 180:
            dim_translations[name] = 'lat'
        elif size == 360:
            dim_translations[name] = 'lon'

    dataset = dataset.rename_dims(dim_translations)

    # Generate lat/lon variables
    lat_data = np.array([i for i in range(-90, 90)], np.int_)
    lat = xr.Variable('lat', lat_data, {
        'standard_name': 'latitude',
        'long_name': 'latitude',
        'valid_min': -90,
        'valid_max': 90
    })

    lon_data = np.array([i for i in range(-180, 180)], np.int_)
    lon = xr.Variable('lon', lon_data, {
        'standard_name': 'longitude',
        'long_name': 'longitude',
        'valid_min': -180,
        'valid_max': 180
    })

    dataset['lat'] = lat
    dataset['lon'] = lon

    return dataset