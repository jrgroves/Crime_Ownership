# Reads in and Compiles the Crime Data from Muritala

#By: Jeremy Groves
#Date: January 13, 2026

rm(list=ls())

library(readxl)
library(tidyverse)
library(tidycensus)
library(sf)

map <- read_sf("./Build/Input/Map/County_Bdy.shp")

acs.map <- get_acs(geography = "tract",
                   variables = "B01002_001",
                   state = "29",
                   county = "189",
                   year = 2015,
                   geometry = TRUE
  
)


years <- seq(2015,2022,1)

for(i in years){
  temp <- read_xlsx(paste0("./Build/Input/CrimeData/PersonCrime_",i,".xlsx"))
  temp$year <- i
  
  ifelse(i==2015, TEMP <- temp, TEMP <- bind_rows(TEMP, temp))
}

test <- read.csv(file = "./Build/Input/CrimeData/STLC Crime.csv", header = TRUE, as.is = TRUE)

per_crime <- TEMP %>%
  mutate(latitute = coalesce(latitude, Y),
         longitude = coalesce(longitude, X),
         count = coalesce(count, Count)) %>%
  select(count, year, latitute, longitude)


per_crime_map <- st_as_sf(per_crime, coords = c("longitude", "latitute"),
                      crs = st_crs(acs.map)) %>%
  st_intersection(., acs.map) %>%
  select(-c(variable, estimate, moe))


for(i in years){
  temp <- read_xlsx(paste0("./Build/Input/CrimeData/PropertyCrime_",i,".xlsx"))
  temp$year <- i
  
  ifelse(i==2015, TEMP <- temp, TEMP <- bind_rows(TEMP, temp))
}

prop_crime <- TEMP %>%
  mutate(latitute = coalesce(latitude, Y),
         longitude = coalesce(longitude, X)) %>%
  select(Count, year, latitute, longitude) %>%
  rename(count = Count)

prop_crime_map <- st_as_sf(prop_crime, coords = c("longitude", "latitute"),
                          crs = st_crs(acs.map)) %>%
  st_intersection(., acs.map) %>%
  select(-c(variable, estimate, moe))

rm(temp, TEMP)

save(per_crime, prop_crime, file = "./Build/Output/crime.RData")

agg_prop_crime <- prop_crime_map %>%
  st_drop_geometry() %>%
  aggregate(count ~ year + GEOID + NAME, FUN = sum)%>%
  full_join(., acs.map, by = c("GEOID", "NAME")) %>%
  select(-c(variable, estimate, moe))%>%
  filter(!is.na(year)) %>%
  st_as_sf()

agg_per_crime <- per_crime_map %>%
  st_drop_geometry() %>%
  aggregate(count ~ year + GEOID + NAME, FUN = sum)%>%
  full_join(., acs.map, by = c("GEOID", "NAME")) %>%
  select(-c(variable, estimate, moe)) %>%
  st_as_sf()

ggplot(agg_per_crime) +
  geom_sf(aes(fill = count)) +
  facet_wrap(~ year)

ggplot(agg_prop_crime) +
  geom_sf(aes(fill = count)) +
  facet_wrap(~ year)

