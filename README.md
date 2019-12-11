# stitching-spark
Reconstructing large microscopy images from overlapping image tiles on a high-performance Spark cluster.

The code is based on the Stitching plugin for Fiji https://github.com/fiji/Stitching

## Usage

### 1. Building the package

Clone the repository with submodules:

```bash
git clone --recursive https://github.com/saalfeldlab/stitching-spark.git
```

If you have already cloned the repository, run this after cloning to fetch the submodules:
```bash
git submodule update --init --recursive
```

The application can be executed on Janelia cluster or locally. Build the package for the desired execution environment:

<details>
<summary><b>Compile for running on Janelia cluster</b></summary>

```bash
python build.py
```
</details>

<details>
<summary><b>Compile for running on local machine</b></summary>

```bash
python build-spark-local.py
```
</details>
<br/>

The scripts for starting the application are located under `startup-scripts/spark-janelia` and `startup-scripts/spark-local`, and their usage is explained in the next steps.

If running locally, you can access the Spark job tracker at http://localhost:4040/ to monitor the progress of the tasks.

#### If running on public platforms such as AWS or Google Cloud:
* Compile with `python build.py`. This will include embed required dependencies into the final package, except for the Spark which is provided by the respective target platform at runtime.
* For running the pipeline, refer to the wiki page [Running on Google Cloud](https://github.com/saalfeldlab/stitching-spark/wiki/Running-on-Google-Cloud)
* The currently used Spark version is **2.3.1** — make sure you're requesting the same version when submitting a job


### 2. Preparing input tile configuration files

The application uses JSON to store metadata about tile configurations. These metadata include:
* list of all tiles in the acquisition grouped by channel
* position and size of each tile in pixels
* pixel resolution (physical size of the pixel/voxel) in nanometers
* image data type

Example of the JSON metadata file (one JSON file per image channel):

*ch0.json:*
```json
[
{
  "index" : 0,
  "file" : "FCF_CSMH__54383_20121206_35_C3_zb15_zt01_63X_0-0-0_R1_L086_20130108192758780.lsm.tif",
  "position" : [0.0, 0.0, 0.0],
  "size" : [991, 992, 880],
  "pixelResolution" : [0.097,0.097,0.18],
  "type" : "GRAY16"
},
{
  "index" : 1,
  "file" : "FCF_CSMH__54383_20121206_35_C3_zb15_zt01_63X_0-0-0_R1_L087_20130108192825183.lsm.tif",
  "position" : [700, -694.0887500300357, -77.41783189603937],
  "size" : [991, 992, 880],
  "pixelResolution" : [0.097,0.097,0.18],
  "type" : "GRAY16"
}
]
```

The application provides automated converters for commonly used formats, but in general case an input metadata file needs to be converted or generated (depending on how you store metadata for your acquisitions).

#### Zeiss Z1
The parser requires an .mvl metadata file. Image tiles can be stored in separate .czi files (one 4D image file per tile that includes all channels), or in a single .czi file.
<details>
<summary><b>Run on Janelia cluster</b></summary>

```
spark-janelia/parse-zeiss-z1-metadata.py \
  -i <path to metadata.mvl> \
  -b <path to directory with image files> \
  -f <images.czi for single file, or image%d.czi for multiple files that contain an index> \
  -r <voxel size in nanometers, for example, 0.114,0.114,0.996>
```
</details>
<details>
<summary><b>Run on local machine</b></summary>

```
spark-local/parse-zeiss-z1-metadata.py \
  -i <path to metadata.mvl> \
  -b <path to directory with image files> \
  -f <images.czi for single file, or image%d.czi for multiple files that contain an index> \
  -r <voxel size in nanometers, for example, 0.114,0.114,0.996>
```
</details>

This will create a single `tiles.json` file in the same directory as the existing .mvl file. Separate JSON tile configuration files for each channel will be created in the next step when the images are split into channel during conversion.

#### ImageList.csv objective-scan acquisitions
*ImageList.csv* metadata file lists image tile filenames in all channels and contains stage and objective coordinates of each tile. Image tiles are expected to be stored as .tif files (separate files for each channel).

Objective coordinates from the metadata file are converted into pixel coordinates based on the provided physical size of a voxel (in nanometers) and axis mapping which specifies flips and swaps between the two coordinate systems. For example, `-y,x,z` would mean that the objective X and Y should be swapped, and objective Y should be flipped.

<details>
<summary><b>Run on Janelia cluster</b></summary>

```
spark-janelia/parse-imagelist-metadata.py \
  -i <path to ImageList.csv> \
  -b <path to directory with image files> \
  -r <voxel size in nanometers, for example, 0.097,0.097,0.18> \
  -a <axis mapping from objective coordinates to pixel coordinates, for example, -y,x,z> \
  [--skipMissingTiles to exclude non-existing tile images from configuration instead of raising an error]
```
</details>
<details>
<summary><b>Run on local machine</b></summary>

```
spark-local/parse-imagelist-metadata.py \
  -i <path to ImageList.csv> \
  -b <path to directory with image files> \
  -r <voxel size in nanometers, for example, 0.097,0.097,0.18> \
  -a <axis mapping from objective coordinates to pixel coordinates, for example, -y,x,z> \
  [--skipMissingTiles to exclude non-existing tile images from configuration instead of raising an error]
```
</details>

This will create a number of JSON configuration files (one per channel), named as `488nm.json`, `560nm.json`, etc. if the corresponding laser frequency can be parsed from the image filenames, or simply `c0.json`, `c1.json`, etc. otherwise. 


### 3. Conversion of image tiles into N5

The application requires to convert all tiles in the acquisition into [N5](https://github.com/saalfeldlab/n5) in order to make the processing in the next steps faster. This also allows to work with different image file formats more easily.

#### .czi (Zeiss Z1) -> N5:

<details>
<summary><b>Run on Janelia cluster</b></summary>

```
spark-janelia/convert-czi-tiles-n5.py \
  <number of cluster nodes> \
  -i <path to tiles.json created in the previous step> \
  [--blockSize to override the default block size 128,128,64]
```
</details>
<details>
<summary><b>Run on local machine</b></summary>

```
spark-local/convert-czi-tiles-n5.py \
  -i <path to tiles.json created in the previous step> \
  [--blockSize to override the default block size 128,128,64]
```
</details>

This will convert the images into N5 and will create new tile configuration files that correspond to the converted tiles. The new configuration files will be named as `c0-n5.json`, `c1-n5.json`, etc. and should be used as inputs in the next steps.

#### .tif -> N5:

<details>
<summary><b>Run on Janelia cluster</b></summary>

```
spark-janelia/convert-tiff-tiles-n5.py \
  <number of cluster nodes> \
  -i 488nm.json -i 560nm.json ... \
  [--blockSize to override the default block size 128,128,64]
```
</details>
<details>
<summary><b>Run on local machine</b></summary>

```
spark-local/convert-tiff-tiles-n5.py \
  -i 488nm.json -i 560nm.json ... \
  [--blockSize to override the default block size 128,128,64]
```
</details>

This will convert the images into N5 and will create new tile configuration files that correspond to the converted tiles. The new configuration files will be named as `488nm-n5.json`, `488nm-n5.json`, etc. and should be used as inputs in the next steps.



### 4. Flatfield estimation

<details>
<summary><b>Run on Janelia cluster</b></summary>

```
spark-janelia/flatfield.py <number of cluster nodes> -i 488nm-n5.json -i 560nm-n5.json ...
```
</details>

<details>
<summary><b>Run on local machine</b></summary>

```
spark-local/flatfield.py -i 488nm-n5.json -i 560nm-n5.json ...
```
</details>

This will create a folder for each channel named such as `488nm-flatfield/` near the provided input files. After the application is finished, it will store two files `S.tif` and `T.tif` in each of the created folders (the brightfield and the offset respectively).
The next steps will detect the flatfield folder and will automatically use the estimated flatfields to perform the flatfield correction on the fly.

The full list of available parameters for the flatfield script is available [here](https://github.com/saalfeldlab/stitching-spark/wiki/Flatfield-parameters).

### 5. Stitching

<details>
<summary><b>Run on Janelia cluster</b></summary>

```
spark-janelia/stitch.py <number of cluster nodes> -i 488nm-n5.json -i 560nm-n5.json ...
```
</details>

<details>
<summary><b>Run on local machine</b></summary>

```
spark-local/stitch.py -i 488nm-n5.json -i 560nm-n5.json ...
```
</details>

This will run the stitching performing a number of iterations until it cannot improve the solution anymore. The images channels will be averaged on-the-fly before computing pairwise shifts in order to get higher correlations because of denser signal.

As a result, it will create files `488nm-n5-final.json`, `560nm-n5-final.json`, etc. near the input tile configuration files.
It will also store a file named `optimizer.txt` that will contain the statistics on average and max errors, number of retained tiles and edges in the final graph, and cross correlation and variance threshold values that were used to obtain the final solution.

The current stitching method is iterative translation-based (improving the solution by building the prediction model).
The pipeline incorporating a higher-order model is currently under development in the `split-tiles` branch.

The full list of available parameters for the stitch script is available [here](https://github.com/saalfeldlab/stitching-spark/wiki/Stitching-parameters).

### 6. Exporting

<details>
<summary><b>Run on Janelia cluster</b></summary>

```
spark-janelia/export.py <number of cluster nodes> -i 488nm-n5-final.json -i 560nm-n5-final.json ...
```
</details>

<details>
<summary><b>Run on local machine</b></summary>

```
spark-local/export.py -i 488nm-n5-final.json -i 560nm-n5-final.json ...
```
</details>

This will generate an [N5](https://github.com/saalfeldlab/n5) export under `export.n5/` folder. The export is fully compatible  with [N5 Viewer](https://github.com/saalfeldlab/n5-viewer) for browsing.

The most common optional parameters are:
* `--blending`: smoothes transitions between the tiles instead of making a hard cut between them
* `--fill`: fills the extra space with the background intensity value instead of 0

The full list of available parameters for the export script is available [here](https://github.com/saalfeldlab/stitching-spark/wiki/Export-parameters).

### 7. Converting N5 export to slice TIFF

<details>
<summary><b>Run on Janelia cluster</b></summary>

```
spark-janelia/n5-slice-tiff.py \
  <number of cluster nodes> \
  -i <path to export.n5 directory> \
  [-s <index of scale level, 0 means full resolution>] \
  [--compress to enable LZW compression. Can be much slower than uncompressed] \
  [--sameDir to prepend all filenames with channel and store all slices in the same directory] \
  [--leadingZeroes to pad slice indices in filenames with leading zeroes]
```
</details>

<details>
<summary><b>Run on local machine</b></summary>

```
spark-local/n5-slice-tiff.py \
  -i <path to export.n5 directory> \
  [-s <index of scale level, 0 means full resolution>] \
  [--compress to enable LZW compression. Can be much slower than uncompressed] \
  [--sameDir to prepend all filenames with channel and store all slices in the same directory] \
  [--leadingZeroes to pad slice indices in filenames with leading zeroes]
```
</details>

This will create a directory named such as `slice-tiff-s0` based on the requested scale level index, where the generated stitched N5 export will be converted into a series of slice TIFF images. The resulting TIFF images will be XY slices.
