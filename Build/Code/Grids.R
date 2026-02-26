
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
map <- read_sf("./Build/Input/Map/County_Bdy.shp") %>%
  st_transform(., st_crs(parcel))

grid <- st_make_grid(map, n = 100)

grid <- grid %>%
  st_sf(.)%>%
  mutate(grid_id = 1:length(lengths(grid)))

year <- 2023

test <- crime.map %>%
  filter(year == 2023) %>%
  st_transform(., st_crs(parcel)) %>%
  st_intersection(., grid) %>%
  select(-c(occurred)) %>%
  st_drop_geometry() %>%
  group_by(grid_id, OffenseCategory, year) %>%
    summarize(count = n()) %>%
  ungroup()

rm(crime, crime.map, parcel, tract.crime, tract.map, map)

temp <- test %>%
  filter(OffenseCategory !="N/A") %>%
  pivot_wider(id_cols = c(grid_id,year),  values_from = count, values_fill = 0,
              names_from = OffenseCategory) 

test2 <- grid %>%
  left_join(., temp, by = "grid_id") %>%
  mutate(year = 2023,
         Property = replace_na(Property, 0),
         Property = case_when(Property == 0 ~ NA,
                              TRUE ~ Property / mean(Property)),
         Person = replace_na(Person, 0),
         Person = case_when(Person == 0 ~ NA,
                              TRUE ~ Person / mean(Person)),
         Society = replace_na(Society, 0),
         Society = case_when(Society == 0 ~ NA, 
                             TRUE ~ Society / mean(Society)))
         
ggplot(test2) +
  geom_sf(aes(fill = Property)) + 
  scale_fill_gradient2(midpoint = 100, na.value = "gray66")
         

load("./Build/Input/Own10.RData")

OWN2 <- OWN %>%
  filter(year == 2023) %>%
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
  select(parid, year, xcoord, ycoord, class, owner, nonowner, ll_city, ll_zip, ll_state, corporate, trustee, nonprofit, reown, 
         partnership, private, hoa, muni) %>%
  st_as_sf(., coords = c("xcoord", "ycoord"), crs = st_crs(grid)) 

test3 <- OWN2 %>%
  st_intersection(., grid) %>%
  st_drop_geometry() %>%
  group_by(grid_id, year) %>%
    summarise(across(c(owner:muni), ~mean(.))) %>%
  ungroup()
  

test4 <- grid %>%
  left_join(., test3, by = "grid_id") 






ggplot(test4) +
  geom_sf(aes(fill = corporate))+
  scale_fill_gradient2(midpoint = .5, breaks = seq(0,1,.20), na.value = "gray")
