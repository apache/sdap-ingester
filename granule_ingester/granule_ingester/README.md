## Plugin Processor Architecture
The operator can write a class that inherits from the `TileProcessor` class and implements the abstract function `process`, which among other things, takes in the NexusTile(`nexusproto.DataTile_pb2.NexusTile` object)) generated with default configurations and the NC4 Dataset(`xarray.Dataset` object), and allows the user to add further modifications to how granule data is saved.  

Any additional transformation the operator needs to accomplish must be done in this `process` method, which is what is ultimately called in the ingestion pipeline.  Helper functions are suggested for breaking up complex procedures.

The custom code file would be copied into the ingestion pods via the helm chart (see chart for local and mount paths).

Example: `KelvinToCelsiusProcessor`
This processor checks the units of the saved variable.  If it is some form of Kelvin, it automatically converts all of the temperature measurements to Celsius by subtracting 273.15 from each data point.  The transformed data then replaces the default (untransformed) values and the processor returns the modified tile.

#### TODO Add configuration option for unusual representations of temperature units.


## Building the Docker image
From `incubator-sdap-ingester`, run:

    $ docker build . -f granule_ingester/docker/Dockerfile -t nexusjpl/granule-ingester
