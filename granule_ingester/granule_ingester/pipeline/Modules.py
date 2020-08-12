from granule_ingester.granule_loaders import GranuleLoader
from granule_ingester.processors import (EmptyTileFilter, GenerateTileId,
                                         KelvinToCelsius,
                                         TileSummarizingProcessor)
from granule_ingester.processors.reading_processors import (
    EccoReadingProcessor, GridReadingProcessor)
from granule_ingester.slicers import SliceFileByStepSize

modules = {
    "granule": GranuleLoader,
    "sliceFileByStepSize": SliceFileByStepSize,
    "generateTileId": GenerateTileId,
    "EccoReadingProcessor": EccoReadingProcessor,
    "GridReadingProcessor": GridReadingProcessor,
    "tileSummary": TileSummarizingProcessor,
    "emptyTileFilter": EmptyTileFilter,
    "kelvinToCelsius": KelvinToCelsius,
}
