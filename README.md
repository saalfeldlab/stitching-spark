# stitching-spark
Reconstruct big images from overlapping tiled images on a Spark cluster.

The code is based on the Stitching plugin for Fiji https://github.com/fiji/Stitching

#### Input
The application requires an input file containing the registered tiles configuration. For convenience, it should be a JSON formatted as follows:
```
[
{
  file : "FCF_CSMH__54383_20121206_35_C3_zb15_zt01_63X_0-0-0_R1_L086_20130108192758780.lsm.tif",
  position : [0.0, 0.0, 0.0],
  size : [991, 992, 880]
},
{
  file : "FCF_CSMH__54383_20121206_35_C3_zb15_zt01_63X_0-0-0_R1_L087_20130108192825183.lsm.tif",
  position : [716.932762003862, -694.0887500300357, -77.41783189603937],
  size : [991, 992, 953]
},
{
  file : "FCF_CSMH__54383_20121206_35_C3_zb15_zt01_63X_0-0-0_R1_L088_20130108192856209.lsm.tif",
  position : [190.0026915705376, -765.0362103552288, -79.91642981085911],
  size : [991, 992, 953]
}
]
```

#### Output
The application performs pairwise stitching of the tiles and then finds the best shifts using global optimization.
As a result, it generates JSON file with updated tile positions in the same format.
The fusion of tile images should be performed separately for now.


# flat-field correction
Run `org.janelia.stitching.IlluminationCorrection` with arguments explained [inline](https://github.com/igorpisarev/stitching-spark/blob/master/src/main/java/org/janelia/stitching/IlluminationCorrectionArguments.java#L18-L36)

```bash
./flintstone.sh \
  10 \
  stitching-spark-0.0.1-SNAPSHOT.jar \
  org.janelia.stitching.IlluminationCorrection \
  -i '/home/igor/3d-fullsize-ch1/config_filtered_ch1_unique.json' \
  --min 0 \
  --max 10000
```

The main method checks if a sub-directory near the input json file exists that contains a histograms directory where historgrams are stored as one file per slice.  If not, histograms are being generated.

Histograms are serialized `TreeMap<Short, Integer>` of sorted original integer intensity values without binning at this time (will change later to be more generic).  Binning considering `min`, `max`, and `bins` is performed at loading the treemaps.  If `min` and `max` are not specified, then the min and max of the entire stack is used (which can be terrribly slow and is probably not useful, so specify them).

Scale hierarchy including half-pixel shifts (ask if you do not know what that means and why and what the heck) is currently hard-coded.

Here is what it does, modify pipeline as needed:

[https://github.com/igorpisarev/stitching-spark/blob/master/src/main/java/org/janelia/stitching/IlluminationCorrection.java#L336]

The pipeline generates output in the a subdirectory `/solution` in the input json file directory.
