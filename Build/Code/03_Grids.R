#Uses the RData file of the MO State Police Crime Data and links it with ownership data after
#aggregating the ownership data by tract

#By: Jeremy Groves
#Date: February 20, 2026

#UPDATED: March 22, 2026 - Muritala
#  1. Fixed bug in grid_id assignment: 1:length(lengths(grid)) always returns 1
#     Corrected to 1:nrow(.) which correctly assigns 1:10000
#  2. Added tracts_sf object for mapping
#  3. Added grid_sf object (filtered to cells with parcel data, EPSG:4326)
#  4. Saves grid_sf and tracts_sf to ./Build/Output/grid_sf.RData

rm(list=ls())

library(tidyverse)
library(sf)


#Load maps and data
  parcel <- read_sf("./Build/Input/Map/Parcels_Current.shp")
  load(file = "./Build/Output/MO_Crime_Prop.RData")
  map <- read_sf("./Build/Input/Map/County_Bdy.shp") %>%
    st_transform(., st_crs(parcel))
  load("./Build/Input/Own10.RData")

#Create Grid
  grid <- st_make_grid(map, n = 100)

  grid <- grid %>%
    st_sf(.) %>%
    mutate(grid_id = 1:nrow(.))  # FIX: was 1:length(lengths(grid)) which returned 1 for all rows

#Join the crime data to the grid map
  crime.grid <- crime.map %>%
    st_transform(crs = st_crs(parcel)) %>%
    st_join(., grid) %>%
    st_drop_geometry() %>%
    aggregate(event ~ year + OffenseCategory + grid_id, FUN = sum)

  own.grid <- OWN %>%
    mutate(ll_city = case_when(co_city == po_city ~ 1,
                               TRUE ~ 0),
           ll_zip = case_when(co_zip == po_zip ~ 1,
                              TRUE ~ 0),
           ll_state = case_when(co_state == "mo" ~ 1,
                                TRUE ~ 0),
           nonowner = case_when(tenure == "NONOWNER" ~ 1,
                                TRUE ~ 0),
           owner = case_when(tenure == "OWNER" ~ 1,
                             TRUE ~ 0)) %>%
    select(parid, year, xcoord, ycoord, class, owner, nonowner, ll_city, ll_zip, ll_state, corporate, trustee,
           nonprofit, reown, partnership, private, hoa, muni) %>%
    st_as_sf(., coords = c("xcoord", "ycoord"), crs = st_crs(parcel)) %>%
    st_join(., grid) %>%
    st_drop_geometry() %>%
    filter(!is.na(grid_id),
           year > 2014) %>%
    mutate(luc.ag = case_when(class == "A" ~ 1,
                              TRUE ~ 0),
           luc.res = case_when(class == "R" | class == "W" | class == "X" ~ 1,
                               TRUE ~ 0),
           luc.com = case_when(class == "C" | class == "Y" | class == "Z" ~ 1,
                               TRUE ~ 0)) %>%
    select(year, grid_id, owner, nonowner, ll_city, ll_zip, ll_state, corporate, trustee, nonprofit, reown, partnership,
           private, hoa, muni, luc.ag, luc.res, luc.com) %>%
    group_by(year, grid_id) %>%
    summarise_all(~ mean(.x, na.rm = TRUE)) %>%
    ungroup()

#Census Tract IDs for Fixed Effects
  grid <- grid %>%
    mutate(g.area = as.numeric(st_area(.)))

  tract.grid <- tract.map %>%
    st_transform(., crs = st_crs(parcel)) %>%
    st_intersection(., grid) %>%
    mutate(area = as.numeric(st_area(.)),
           share = area / g.area) %>%
    st_drop_geometry() %>%
    group_by(grid_id) %>%
      mutate(GEOID = case_when(share == max(share) ~ GEOID,
                               TRUE ~ NA)) %>%
      fill(GEOID, .direction = "downup") %>%
    ungroup() %>%
    select(grid_id, GEOID) %>%
    distinct()

#Save Data for Output to Compile
  save(crime.grid, tract.grid, own.grid, file = "./Build/Output/Grid_data.RData")

# =============================================================================
# ADDED: Create and save grid geometry objects for mapping
# =============================================================================

  # tracts_sf — census tract geometries in WGS84
  tracts_sf <- tract.map %>%
    st_transform(crs = 4326)

  # grid_sf — grid cells with parcel data only, in WGS84
  # Filter AFTER building full grid to preserve correct IDs
  grid_sf <- grid %>%
    filter(grid_id %in% unique(own.grid$grid_id)) %>%
    st_transform(crs = 4326) %>%
    select(grid_id, geometry)

  cat("grid_sf cells:", nrow(grid_sf), "\n")
  cat("tracts_sf rows:", nrow(tracts_sf), "\n")

  save(grid_sf, tracts_sf, file = "./Build/Output/grid_sf.RData")
  cat("grid_sf.RData saved.\n")
