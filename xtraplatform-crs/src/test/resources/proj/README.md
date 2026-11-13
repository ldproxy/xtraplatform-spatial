# Test grid

`de_hvbg_hessen_HeTA2010.tif` is a cropped copy (8.5–8.9 E, 50.0–50.3 N, around Frankfurt am
Main) of the HeTA2010 NTv2 grid for the transformation DHDN → ETRS89 in Hessen, as distributed in
GeoTIFF form by the PROJ data package (`de_hvbg_hessen_HeTA2010.tif`). It is used to test the
selection of coordinate operations for an area of interest.

- Source: Hessische Verwaltung für Bodenmanagement und Geoinformation (HVBG),
  https://www.hvbg.hessen.de
- License: Data licence Germany – attribution – version 2.0, https://www.govdata.de/dl-de/by-2-0
- Modification: cropped with `gdal_translate -projwin 8.5 50.3 8.9 50.0`; values unchanged.
