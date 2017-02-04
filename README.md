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

Run `org.janelia.stitching.StitchingSpark` with arguments explained [inline](https://github.com/igorpisarev/stitching-spark/blob/master/src/main/java/org/janelia/stitching/StitchingArguments.java#L23-L66)

```bash
./flintstone.sh \
  10 \
  stitching-spark-0.0.1-SNAPSHOT.jar \
  org.janelia.stitching.StitchingSpark \
  -i '/home/igor/3d-fullsize-ch1/stitching/ch0-xy/10z/ch0_10z.json' \
  --stitch \
  -v um=0.097,0.097,0.180
```

The application checks if a file 'ch0_10z_pairwise.json' exists. If not, it computes pairwise shifts for all tile pairs in approximate 3D six-neighborhood (i.e. diagonal overlaps are typically ignored, this is subject to change and for parameterization, e.g. how much overlap we consider sufficient to calculate pairwise shift vectors).  Shift vectors are stored in the earlier mentioned pairwise file.  Then, it performs global optimization with the parameters specified (or [hardcoded](https://github.com/igorpisarev/stitching-spark/blob/master/src/main/java/org/janelia/stitching/PipelineStitchingStepExecutor.java#L703)).  Output is saved as 'ch0_10z-final.json'

If the directory of the input json file contains two files `v.tif` and `z.tif`, then they are used as flatfield correction coefficients that are applied to each input tile.  TODO specify this nicely in the configuration, e.g. we have independent and actually pretty different correction fields for each channel.

If you omit the `--stitch` parameter, the job also exports (fuses) the result.


# fusion (export)

Run `org.janelia.stitching.StitchingSpark` with arguments explained [inline](https://github.com/igorpisarev/stitching-spark/blob/master/src/main/java/org/janelia/stitching/StitchingArguments.java#L23-L66)

## TODO
* Should the '-v' flag below actually be '-r'? [Check here](https://github.com/igorpisarev/stitching-spark/blob/master/src/main/java/org/janelia/stitching/StitchingArguments.java#L47-L49)

```bash
./flintstone.sh \
  10 \
  stitching-spark-0.0.1-SNAPSHOT.jar \
  org.janelia.stitching.StitchingSpark \
  -i '/home/igor/3d-fullsize-ch1/stitching/ch0-xy/10z/ch0_10z.json' \
  --fuse \
  -f 256 \
  -v um=0.097,0.097,0.180
```
This generates an export of the stitched volume as specified in the json file.  The export uses '''max-border distance''' as fusion mode, no blending.  It currently exports into the ad-hoc BDV cell file format into the directory of the json input file, e.g. `/home/igor/3d-fullsize-ch1/stitching/ch0-xy/10z/channel0` and generates a json file for the BDV cell file viewer.

As with stitching, if the directory of the input json file contains two files `v.tif` and `z.tif`, then they are used as flatfield correction coefficients that are applied to each input tile.  TODO specify this nicely in the configuration, e.g. we have independent and actually pretty different correction fields fopr each channel.

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
