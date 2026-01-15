# Reads in and Compiles the Crime Data from Muritala

#By: Jeremy Groves
#Date: January 13, 2026

rm(list=ls())

library(readxl)
library(tidyverse)
library(sf)

map <- read_sf("./Build/Input/Map/Parcels_Current.shp")

years <- seq(2015,2022,1)

for(i in years){
  temp <- read_xlsx(paste0("./Build/Input/CrimeData/PersonCrime_",i,".xlsx"))
  temp$year <- i
  
  ifelse(i==2015, TEMP <- temp, TEMP <- bind_rows(TEMP, temp))
}

crime <- TEMP %>%
  mutate(latitute = coalesce(latitude, Y),
         longitude = coalesce(longitude, X),
         count = coalesce(count, Count)) %>%
  select(count, year, latitute, longitude)

crime_map <- st_as_sf(crime, coords = c("longitude", "latitute"),
                      crs = st_crs(map))
stl <- map %>%
  st_make_valid() %>%
  st_union() %>%
  st_make_valid %>%
  st_boundary() 


stl2<-st_sf(stl)
