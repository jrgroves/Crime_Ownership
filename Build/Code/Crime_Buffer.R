#Uses the RData file of the MO State Police Crime Data and links it with ownership data after
#aggregating the ownership data by tract

#By: Jeremy Groves
#Date: February 20, 2026

rm(list=ls())

library(tidyverse)
library(sf)


#Load maps and data
  parcel <- read_sf( "./Build/Input/Map/Parcels_Current.shp")

  load(file = "./Build/Output/MO_Crime_Prop.RData")

  rm(crime, tract.crime, tract.map)

#Create buffers around crime points
  
  buffer <- crime.map %>%
    st_transform(., crs = st_crs(parcel)) %>%
    st_buffer(., dist = 1320)
  
  
  temp <- parcel %>%
    select(PARENT_LOC, geometry) %>%
    rename("parid" = "PARENT_LOC") %>%
    st_centroid()
  
  buffer1320ft <- temp %>%
    st_intersection(., buffer)
  
  save(buffer, buffer1320ft, file = "./Build/Output/buffer.RData")
  
  
  load("./Build/Output/buffer.RData")
  source("./Build/Code/modown.R")
  
  temp <- buffer1320ft %>%
    rename("parid" = "PARID") %>%
    left_join(., OWN, by = c("parid", "year"))
  
  temp <- temp %>%
    filter(!is.na(owner))
  