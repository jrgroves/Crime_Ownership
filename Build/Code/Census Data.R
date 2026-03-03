#Creates the census tract population and other census features

#By: Jeremy Groves
#Date: February 23. 2026

rm(list=ls())

library(tidyverse)
library(tidycensus)
library(sf)

#Load data and maps

parcel <- st_read("./Build/Input/Map/Parcels_Current.shp")
years = seq(2020, 2024) #2025 data not available yet
census <- c("B01001_001", "B06011_001", "B02001_001", "B02001_002", "B02001_003",
            "B02001_005", "B01001_002", "B01001_003", "B01001_004", "B01001_005",
            "B01001_006","B01001_027", "B01001_028", "B01001_029", "B01001_030",
            "B05010_002", "B05010_010", "B11012_001", "B11012_008")
cen.names <- c("Tot_Pop", "Med_Inc", "Race_Tot", "White", "Black","Asian",
               "Male", "male1", "male2", "male3", "male4", "female1",
               "female2", "female3", "female4", "poverty1", "poverty2",
               "Tot_House", "female_HH")

for(i in years){
    temp <- get_acs(geography = "tract",
                    variables = census,
                    state = "29",
                    county = "189",
                    year = i,
                    geometry = TRUE
                       
    )
    temp$year <- i
    ifelse(i == 2020, acs.data <- temp, acs.data <- bind_rows(acs.data, temp))
}

for(i in 1:length(census)){
acs.data <- acs.data %>%
  mutate(variable = case_when(variable == census[i] ~ cen.names[i],
                              TRUE ~ variable))
}

acs <- acs.data %>%
  st_transform(crs = st_crs(parcel)) %>%
  mutate(area = st_area(.)) %>%
  select(GEOID, variable, estimate, year, area) %>%
  st_drop_geometry() %>%
  pivot_wider(id_cols = c("GEOID", "year", "area"), names_from = variable, 
              values_from = estimate) %>%
  mutate(per_male = (Male / Tot_Pop),
         per_u18 = (male1+male2+male3+male4+female1+female2+female3+female4) / Tot_Pop,
         per_wht = White / Tot_Pop,
         per_blk = Black / Tot_Pop,
         per_asn = Asian / Tot_Pop,
         per_oth = 1 - per_wht - per_blk - per_asn,
         per_pov1 = poverty1 / Tot_Pop,
         per_pov2 = poverty2 / Tot_Pop,
         area = area / 27878400,
         per_fhh = female_HH / Tot_House,
         dens = Tot_Pop / area) %>%
  select(GEOID, year, "Tot_Pop", "Med_Inc", area, dens, starts_with("per"))

         

save(acs, file = "./Build/Output/census.RData")
  
    
