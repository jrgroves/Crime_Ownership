# Crime_Ownership
Repo for the Crime and Parcel Ownership Project

## File Descriptions
- Own.RData: This file contains the ownership address and type for each parcel of land in the country from 2000 through 2024 so long as the records are present. There are a handful of cases where the PARID numbers have changed or were originally entered in error. We can find and correct those as needed.
- Sales.RData: This file contains all of the sales of property between the years of 2002 and 2024. While it has been mostly cleaned of invalid or unuseful sales from the original, there may be some that still need removed. The script to create the file is in the code folders and is named Sales.R.
- CenMap.RData: This links each of the parcel IDs in the data to their census GEOID for 2000, 2010, and 2020.
- "./Build/Input/Map/Parcels_Current.shp": This is the shape file from Saint Louis County GIS office from 2025


