#Uses the RData file of the MO State Police Crime Data and links it with ownership data after
#aggregating the ownership data by tract

#By: Jeremy Groves
#Date: February 202, 206

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
                     year = 2015,
                     geometry = TRUE
                     
  )
  
  tracts <- acs.map %>%
    select(GEOID, NAME, geometry)
  
  map <- read_sf("./Build/Input/Map/County_Bdy.shp") %>%
    st_transform(., st_crs(acs.map))
  
  parcel <- read_sf( "./Build/Input/Map/Parcels_Current.shp")
  
  load(file = "./Build/Output/MO_Crime_Prop.RData")

#Map PARID into census tracts

  parcel2 <- parcel %>%
    select(LOCATOR, CENSUS_TRA) %>%
    rename("parid" = "LOCATOR") %>%
    st_drop_geometry()
  
  tracts2 <- tracts %>%
    mutate(CENSUS_TRA = substr(GEOID, 6, 11)) %>%
    st_drop_geometry() %>%
    left_join(., parcel2, by="CENSUS_TRA")
  
#Connect tract ids to OWN data
  
  source("./Build/Code/modown.R")

  own <- OWN %>%
    filter(year > 2014) %>%
    left_join(., tracts2, by = "parid") %>%
    filter(!is.na(CENSUS_TRA))
  rm(OWN)
  
  own.agg <- own %>%
    mutate(parcel = 1) %>%
    select(CENSUS_TRA, year, corporate, trustee, nonprofit, reown, partnership, private, hoa, 
           muni, owner, nonowner, ll_city, ll_zip, ll_state, parcel) %>%
    group_by(CENSUS_TRA, year) %>%
    summarise(across(c(corporate:parcel), ~sum(.))) %>%
    mutate(across(corporate:parcel, ~ .x / parcel))
  
#Connect ownership data to crime data by tracts
  tract.agg <- tract.crime %>%
    filter(OffenseCategory!="N/A") %>%
    mutate(CENSUS_TRA = substr(GEOID, 6, 11)) %>%
    full_join(., own.agg , by = c("CENSUS_TRA", "year")) %>%
    filter(year > 2020) %>%
    filter(!is.na(parcel))
  
  save(tract.agg, file = "./Build/Output/tractagg.RData")
 




