#Uses the RData file of the MO State Police Crime Data and links it with ownership data after
#aggregating the ownership data by tract

#This is run after the MO Crime Read and Map.R script

#By: Jeremy Groves
#Date: February 20, 2026

rm(list=ls())

library(readxl)
library(tidyverse)
library(tidycensus)
library(sf)

#Load data and maps
  acs.map <- get_acs(geography = "tract",
                     variables = "B01002_001",
                     state = "29",
                     county = "189",
                     year = 2020,
                     geometry = TRUE
                     
  )
  
  tracts <- acs.map %>%
    select(GEOID, NAME, geometry)
  
  map <- read_sf("./Build/Input/Map/County_Bdy.shp") %>%
    st_transform(., st_crs(acs.map))
  
  parcel <- read_sf( "./Build/Input/Map/Parcels_Current.shp")
  
  load(file = "./Build/Output/MO_Crime_Prop.RData")

#Map PARID into census tracts
  
  tracts2 <- tracts %>%
    select(-NAME) %>%
    st_transform(., crs = st_crs(parcel))

  parcel2 <- parcel %>%
    select(LOCATOR) %>%
    rename("parid" = "LOCATOR") %>%
    st_centroid() %>%
    st_join(., tracts2) %>%
    st_drop_geometry()
  
#Connect tract ids to OWN data
  
  load("./Build/Input/Own10.RData")

  own <- OWN %>%
    filter(year > 2014) %>%
    mutate(ll_city = case_when(co_city == po_city ~ 1,
                               TRUE ~0),
           ll_zip = case_when(co_zip == po_zip ~ 1,
                              TRUE ~ 0),
           ll_state = case_when(co_state == "mo" ~ 1,
                                TRUE ~ 0),
           nonowner = case_when(tenure == "NONOWNER" ~ 1,
                                TRUE ~ 0)) %>%
    select(-starts_with("po_"), -starts_with("co_"),
           -xcoord, -ycoord, -tenure)  %>%
    left_join(., parcel2, by = "parid", relationship = "many-to-many") %>%
    distinct()%>%
    filter(!is.na(GEOID))
  rm(OWN)
  
  own.agg <- own %>%
    mutate(parcel = 1) %>%
    select(GEOID, year, corporate, trustee, nonprofit, reown, partnership, private, hoa, 
           muni, nonowner, ll_city, ll_zip, ll_state, parcel) %>%
    summarise(across(c(corporate:parcel), ~sum(.)), .by = c(GEOID, year)) %>%
    mutate(across(corporate:parcel, ~ .x / parcel))
  
#Connect ownership data to crime data by tracts
  tract.agg <- tract.crime %>%
    full_join(., own.agg , by = c("GEOID", "year")) %>%
    filter(!is.na(parcel))
  
  save(tract.agg, file = "./Build/Output/tractagg.RData")
 




