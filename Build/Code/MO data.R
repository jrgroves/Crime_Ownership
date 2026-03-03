# Reads in and Compiles the Crime Data from Muritala

#By: Jeremy Groves
#Date: January 13, 2026

rm(list=ls())

library(readxl)
library(tidyverse)
library(tidycensus)
library(sf)

acs.map <- get_acs(geography = "tract",
                   variables = "B01002_001",
                   state = "29",
                   county = "189",
                   year = 2015,
                   geometry = TRUE
                   
)

map <- read_sf("./Build/Input/Map/County_Bdy.shp") %>%
  st_transform(., st_crs(acs.map))

bbox <- st_bbox(map)

test <- read.csv(file = "./Build/Input/CrimeData/STLC Crime.csv", header = TRUE, as.is = TRUE) %>%
  filter(!is.na(OffenseCategory))

crime <- test %>%
  filter(!is.na(latitude),
         !is.na(longitude)) %>%
  select(ObjectID, latitude, longitude, occurred, OffenseCategory) %>%
  mutate(year = as.numeric(substr(occurred, 1, 4)),
         event = 1) %>%
  filter(year > 2020,
         year < 2026,
         between(latitude, bbox[2], bbox[4]),
         between(longitude, bbox[1], bbox[3]))

crime.map <- st_as_sf(crime, coords = c("longitude", "latitude"), crs = st_crs(map))

tract.map <- acs.map %>%
  select(GEOID, geometry) %>%
  st_make_valid()

for(i in seq(2021,2025)){
  temp1 <- crime.map %>%
    filter(year == i)
  
  temp2 <- st_intersection(tract.map, temp1)
  
  temp2$year <- i
  
  ifelse(i==2021, TEMP <- temp2, TEMP <- bind_rows(TEMP, temp2))
}

tract.map <- TEMP

tract.crime <- tract.map %>%
  st_drop_geometry() %>%
  aggregate(event ~ GEOID + year + OffenseCategory, FUN = sum)

save(crime.map, crime, tract.map, tract.crime, file = "./Build/Output/MO_Crime_Prop.RData")
rm(temp1, temp2, TEMP, test, bbox, i)





