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

test <- read.csv(file = "./Build/Input/CrimeData/STLC Crime.csv", header = TRUE, as.is = TRUE)

property <- test %>%
  filter(OffenseCategory == "Property") %>%
  filter(!is.na(latitude),
         !is.na(longitude)) %>%
  select(ObjectID, latitude, longitude, occurred) %>%
  mutate(year = as.numeric(substr(occurred, 1, 4))) %>%
  filter(between(longitude, bbox[1], bbox[3]),
         between(latitude, bbox[2], bbox[4]),
         year > 2020,
         year < 2026)


prop.map <- st_as_sf(property, coords = c("longitude", "latitude"),
                     crs = st_crs(map)) %>%
  mutate(property = 1) %>%
  st_intersection(., acs.map)

ggplot(prop.map) +
  geom_sf(aes(fill = property)) +
  facet_wrap(~year) 

