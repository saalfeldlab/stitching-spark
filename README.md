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
