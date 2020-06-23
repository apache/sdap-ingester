from granule_ingester.processors import *
from granule_ingester.processors.reading_processors import *
from granule_ingester.slicers import *
from granule_ingester.granule_loaders import *

modules = {
    "granule": GranuleLoader,
    "sliceFileByStepSize": SliceFileByStepSize,
    "generateTileId": GenerateTileId,
    "EccoReadingProcessor": EccoReadingProcessor,
    "GridReadingProcessor": GridReadingProcessor,
    "tileSummary": TileSummarizingProcessor,
    "emptyTileFilter": EmptyTileFilter,
    "kelvinToCelsius": KelvinToCelsius
}
