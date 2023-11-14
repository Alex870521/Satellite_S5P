# Satellite_S5P
### NO2 Data Processing Script

This Python script is designed to process and analyze nitrogen dioxide (NO2) data from satellite observations. Here's a breakdown of its main components:

### Data Extraction:
The script uses the extract_data function to filter and extract relevant NO2 data from a given dataset based on predefined geographical conditions (latitude and longitude ranges) and a quality threshold (qa_value).

### Data Interpolation:
The interp_data function utilizes a KD-tree algorithm to find the nearest neighbors in a 2D array of latitude and longitude. It then performs linear interpolation using the map_coordinates function to estimate NO2 values at specified target coordinates.

### Main Loop:

The script iterates through folders corresponding to different months of a specified year, each containing NetCDF files of satellite observations.
For each NetCDF file, it extracts and interpolates NO2 data and stores the results.

### Data Aggregation:
The script aggregates the processed NO2 data from multiple files within a month, calculating the average.

### Output:
The processed NO2 data is stored in NetCDF format, with each file representing a month of observations.

### Folder Skipping:
The script checks if the output NetCDF file already exists. If it does, it skips processing that particular folder to avoid unnecessary computations.

### Usage:
The script is designed to be run for a specific year, processing monthly satellite observations of NO2 data and generating NetCDF files for further analysis or visualization.

### Dependencies:
The script relies on various Python libraries, including NumPy, xarray, scipy, and netCDF4, for efficient data manipulation and processing.
This script is particularly useful for researchers or analysts working with satellite-derived NO2 data, allowing them to extract, interpolate, and aggregate information for further study.
