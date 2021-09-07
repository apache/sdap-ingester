import logging

logger = logging.getLogger(__name__)
band = 'band'


class MultiBandUtils:
    BAND = 'band'

    @staticmethod
    def move_band_dimension(single_data_subset_dims):
        updated_dims = single_data_subset_dims + [MultiBandUtils.BAND]
        logger.debug(f'updated_dims: {updated_dims}')
        return updated_dims, tuple([k for k in range(1, len(updated_dims))] + [0])
        return tuple(new_dimensions)