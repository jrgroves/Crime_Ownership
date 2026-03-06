#Opens the Missouri Crime Data from the State Police and links it with census tracts. Also uses data collected by
#Muritala to add the years 2015 through 2020 to the Missouri State Police Data.


#Jeremy R. Groves
#Created: February 20, 2026
#Updated: March 3, 2026 - Added data from Muritala and replace st_intersection with st_join for speed

rm(list=ls())

library(readxl)
library(tidyverse)
library(tidycensus)
library(sf)

#Load basics

  #Download tract map to get county boundaries becomes some locations are outside the county
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
    
    bbox <- st_bbox(map)

  #Read in the data from the MO State Police
  
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
             between(longitude, bbox[1], bbox[3])) %>%
      select(-c(ObjectID, occurred)) %>%
      filter(OffenseCategory != "Society",
             OffenseCategory != "N/A")

  #Read Rest of Data from Muritala 

    for(i in seq(2015,2020)){
      temp <- read_excel(paste0("./Build/Input/CrimeData/PersonCrime_",i,".xlsx")) 
      temp$year <- i
      temp$OffenseCategory = "Person"
      
      ifelse(i==2015, CRIME <- temp, CRIME <- bind_rows(CRIME, temp))
      
      temp <- read_excel(paste0("./Build/Input/CrimeData/PropertyCrime_",i,".xlsx")) 
      temp$year <- i
      temp$OffenseCategory = "Property"
      
      CRIME <- bind_rows(CRIME, temp)
    }
    
  #Merges the two datasets to one larger set
    CRIME <- CRIME %>%
      rename("event" = "Count",
             "latitude" = "Y",
             "longitude" = "X") %>%
      bind_rows(., crime)
  
  rm(test, temp, crime, bbox, i)
  
#Link to Maps
  #Create SF object from crime data
    
    crime.map <- st_as_sf(CRIME, coords = c("longitude", "latitude"), crs = st_crs(map))

  #Prepare the census tract map (NOTE, Interacting with 2020 census tracts)        
    tract.map <- acs.map %>%
      select(GEOID, geometry) %>%
      st_make_valid()
    
  #Do spatial join of crime locations with census tracts
    tract.crime <- st_join(crime.map, tract.map) %>%
      st_drop_geometry() %>%
      aggregate(event ~ GEOID + year + OffenseCategory, FUN = sum)

save(crime.map, CRIME, tract.map, tract.crime, file = "./Build/Output/MO_Crime_Prop.RData")