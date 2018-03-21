## How to use olive_boxes.py

`from impresso_commons.images import olive_boxes as boxes`

1) For each page, call the `get_scale_factor`     
2) For each box of the page, call the function `compute_box`

info.txt files of image conversion are in the images folder, with canonical file tree.

## Documentation

### How to recompute an Olive box between 2 images:

1) Starting point

A box in Olive is like `262 624 73 26`, where:   

```  
262 = x upper left (x_ul)
624 = y upper left (y_ul)
73  = x lower right (x_lr)
26  = y lower right (y_lr)
```

2) Arrival point

The region parameter of IIIF Image API also requires 4 coordinates, but with:

`x,y,width,height` 

### Computing scale factor between source and dest image

Given:

- a source image of dimensions `x1 * y1` 
- a dest image of dimensions `x2 * y2`

Scaling factor (SF) (of x) = `x1 / x2`

### Computing the new coordinates

```
x_dest = x_ul source * SF
y_dest = y_ul source * SF
w = (xlr - xul) * SF
h = (ylr - yul) * SF
```


### Strategies to choose the source images

1) When jp2 acquired from tif:
use strategy 1 of example 1

2) when several pngs

- check if resolution indicated in Ar.xml file is the same as the one of the png used for conversion to jp2.
- if identical then compute using the dimensions of this image
- else: log it

3) when one png

take the resolution of the unique existing image => results look OK but needs to be double-check large scale

4) when one jpg

take the resolution of the unique existing image => results look OK but needs to be double-check large scale



### Example 1 with `GDL-1900-01-10-a-p0001` (tif)

box = `262 624 335 650` for the word *janvier* 

1) Destination
.tif was converted to .jp2, both have dimensions `5177 * 7108`

2) Source
What's present in the folder:   

```
the tif file
Pg001.png with dimensions 984 * 1351
Pg001_72.png with dimensions 1243 * 1706
Pg001_144.png with dimensions 2485 * 3412
```

3) What is written in Olive's file

- in `Pg001.xml`:    

```
in DataFiles: TYPE="PAGE_IMG" PRESENT="1"    
in DataFiles: XRESOLUTION_DPI="300"  => tif resolution for Vincent    
in Meta: SOURCE_TYPE="TIFF" SOURCE_RESOLUTION="72" IMAGES_RESOLUTION="144"
```
		
- in `Ar00100.xml`: 

```
	- in tag Meta: IMAGES_RESOLUTION="144" => dest resolution for Vincent
	- in Meta: PAGE_WIDTH="984" PAGE_HEIGHT="1351" DEFAULT_IMG_EXT="png"
```

- how Vincent computes the scale factor:    
`IMAGES_RESOLUTION` of the Ar `/` `XRESOLUTION_DPI` of the Pg  


4) Let's try

*Without changing nothing*:

`262 624 335 650` => `262 624 73 26`

`http://dhlabsrv17.epfl.ch/iiif_impresso/GDL-1900-01-10-a-p0001/262,624,73,26/full/0/default.jpg`

*Strategy 1: Using Vincent' method*

```
SF = 300 / 144 = 2.08
x = 262 * 2.08 = 544 
y = 624 * 2.08 = 1299 
w = (335 - 262) * 2.08 = 73 * 2.08 = 152
h = (650 - 624) * 2.08 = 26 * 2.08 = 54
```

Testing
`http://dhlabsrv17.epfl.ch/iiif_impresso/GDL-1900-01-10-a-p0001/544,1299,152,54/full/0/default.jpg`


*Strategy 2: Using information from `Meta:  IMAGES_RESOLUTION="144"` in `Pg001.xml` and in `Ar00100.xml`*

We can deduce that we should take the dimensions of the `Pg001_144.png` as source image.

```   
SF = 5177 / 2485 = 2.08
x = 262 * 2.08 = 544 
y = 624 * 2.08 = 1299 
w = 73 * 2.08 = 152  
h = 26 * 2.08 = 54
```

Testing:
`http://dhlabsrv17.epfl.ch/iiif_impresso/GDL-1900-01-10-a-p0001/544,1299,152,54/full/0/default.jpg`


### Example 2 with `EXP-1930-06-10-a-p0001` (several pngs)

box = `85 652 234 708` for the word *Centenaire* 

1) Destination:    
the highest resolution png was converted to .jp2, both have dimensions `2880 * 4267`

2) Source:
What's present in the folder:   

```
Pg001.png with dimensions 1280 * 1897
Pg001_103.png with dimensions 1648 * 2442
Pg001_180.png with dimensions 2880 * 4267
```

3) What is written in Olive's file:

- in `Pg001.xml`:   

``` 
in DataFiles: TYPE="PAGE_IMG" PRESENT="0"   <= 0 here
in Meta: SOURCE_TYPE="TIFF" SOURCE_RESOLUTION="72" IMAGES_RESOLUTION="180"
```
		
- in `Ar00100.xml`: 

```
in tag Meta: IMAGES_RESOLUTION="180" 
in Meta: PAGE_WIDTH="1280" PAGE_HEIGHT="1897" DEFAULT_IMG_EXT="png"
```

4) Let's try:

*Without changing nothing*

`85 652 234 708` => `85 652 150 56`

`http://dhlabsrv17.epfl.ch/iiif_impresso/EXP-1930-06-10-a-p0001/85,652,150,56/full/0/default.jpg`

=> seems that when `IMAGES_RESOLUTION="180"` corresponds to our chosen image resolution to convert from, no need to scale.



### Example 3 with `EXP-1889-07-01-a-p0001` (several pngs)

box = `1047 1006 1173 1036` for the word *NEUCHATEL* in `Ad00103.xml`

1) Destination:    
the highest resolution png was converted to .jp2, both have dimensions `1882 * 2694`

2) Source:
What's present in the folder:   

```
Pg001.png with dimensions `850 * 1216`
Pg001_105.png with dimensions `1372 * 1965`
Pg001_144.png with dimensions `1882 * 2694`
```

3) What is written in Olive's file:

- in `Pg001.xml`:    

```
DataFiles: TYPE="PAGE_IMG" PRESENT="0"   <= 0 here   
Meta: SOURCE_TYPE="TIFF" SOURCE_RESOLUTION="72" IMAGES_RESOLUTION="144"   
```
		
- in `Ad00103.xml`:
 
```
in tag Meta: IMAGES_RESOLUTION="144"    
in Meta: PAGE_WIDTH="850" PAGE_HEIGHT="1216" DEFAULT_IMG_EXT="png"
```


4) Let's try:

*Without changing nothing*

`1047 1006 1173 1036` => `1047 1006 126 30`

`http://dhlabsrv17.epfl.ch/iiif_impresso/EXP-1889-07-01-a-p0001/1047,1006,126,30/full/0/default.jpg`

=> seems that when `IMAGES_RESOLUTION="144"` corresponds to our chosen image resolution to convert from, no need to scale.

Let's try with the ful article/ad:

box = `762 672 1424 1737` => `762 672 662 1065`

`http://dhlabsrv17.epfl.ch/iiif_impresso/EXP-1889-07-01-a-p0001/762,672,662,1065/full/0/default.jpg`

### Example 4 with `xxx` (one png)


### Example 5 with `LCE-1868-08-02-a-p0001` (one jpg)


box = `483 502 556 517` for the word *Hambourg* in `Ar00100.xml`

1) Destination:    
 .jp2, same resolution as jpg, both have dimensions `1457 * 2003`

2) Source:
What's present in the folder:   

- a `Pg001.png` with dimensions `1457 * 2003`


3) What is written in Olive's file:

- in `Pg001.xml`:    
	- in `DataFiles`: `TYPE="PAGE_IMG" PRESENT="0"`   <= 0 here
	- in `Meta`: `SOURCE_TYPE="TIFF" SOURCE_RESOLUTION="72" IMAGES_RESOLUTION="144"`
		
- in `Ad00103.xml`: 
	- in tag `Meta`: `IMAGES_RESOLUTION="144"` 
	- in `Meta`: `PAGE_WIDTH="1457" PAGE_HEIGHT="2003" DEFAULT_IMG_EXT="png"`

4) Let's try:

*Without changing nothing*

`483 502 556 517` => `483 502 73 15`

`http://dhlabsrv17.epfl.ch/iiif_impresso/LCE-1868-08-02-a-p0001/483,502,73,15/full/0/default.jpg`

*trying with another box*

`<W BOX="628 502 680 517" STYLE_REF="4">Leipzig</W>` => `628 502 52 15`

`http://dhlabsrv17.epfl.ch/iiif_impresso/LCE-1868-08-02-a-p0001/628,502,52,15/full/0/default.jpg`

`<Primitive BOX="12 453 1378 519" ID="Ar0010000" SEQ_NO="0" TOC_ENTRY_ID="1" DEPTH_LEVEL="1">` => `12 453 1366 66`

`http://dhlabsrv17.epfl.ch/iiif_impresso/LCE-1868-08-02-a-p0001/12,453,1366,66/full/0/default.jpg`

=> looks all ok

	
	
	
	
	
	
	
	
	
	
	
	



	


