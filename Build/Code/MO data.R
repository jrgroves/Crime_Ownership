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

test <- read.csv(file = "./Build/Input/CrimeData/STLC Crime.csv", header = TRUE, as.is = TRUE)

property <- test %>%
  filter(OffenseCategory == "Property") %>%
  filter(!is.na(latitude),
         !is.na(longitude)) %>%
  select(ObjectID, latitude, longitude, occurred) %>%
  mutate(year = as.numeric(substr(occurred, 1, 4))) %>%
  filter(year > 2019,
         latitude > 0) 

prop.map <-   st_as_sf(property, coords = c("longitude", "latitude"),
                       crs = st_crs(acs.map)) %>%
  mutate(count = 1) %>%
  st_intersection(acs.map) %>%
  select(-c(variable, estimate, moe))


ggplot(prop.map) +
  geom_sf(aes(color = count)) +
  facet_wrap(~ year)

%>%
  st_intersection(., acs.map) %>%
  select(-c(variable, estimate, moe))

agg_prop_crime <- prop_crime_map %>%
  st_drop_geometry() %>%
  aggregate(count ~ year + GEOID + NAME, FUN = sum)%>%
  full_join(., acs.map, by = c("GEOID", "NAME")) %>%
  select(-c(variable, estimate, moe))%>%
  filter(!is.na(year)) %>%
  st_as_sf()

ggplot(agg_prop_crime) +
  geom_sf(aes(fill = count)) +
  facet_wrap(~ year)







person <- test %>%
  filter(OffenseCategory == "Person")
