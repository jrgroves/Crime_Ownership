#Creates the census tract population and other census features

#By: Jeremy Groves
#Date: February 23. 2026

rm(list=ls())

library(tidyverse)
library(tidycensus)
library(sf)

#Load base maps and download ACS variables
    
    parcel <- st_read("./Build/Input/Map/Parcels_Current.shp")
    years = seq(2015, 2024) #2025 data not available yet
    census <- c("B01001_001", "B19013_001", "B02001_001", "B02001_002", "B02001_003",
                "B02001_005", "B01001_002", "B01001_003", "B01001_004", "B01001_005",
                "B01001_006","B01001_027", "B01001_028", "B01001_029", "B01001_030",
                "B05010_002", "B05010_010", "B11001_001", "B11001_006", "B11001_007",
                "B11001_004")
    cen.names <- c("Tot_Pop", "Med_Inc", "Race_Tot", "White", "Black","Asian",
                   "Male", "male1", "male2", "male3", "male4", "female1",
                   "female2", "female3", "female4", "poverty1", "poverty2",
                   "Tot_House", "female_HH", "nonfam", "other_fam")
    
    for(i in years){
        temp <- get_acs(geography = "tract",
                        variables = census,
                        state = "29",
                        county = "189",
                        year = i,
                        geometry = TRUE
                           
        )
        temp$year <- i
        ifelse(i == 2015, acs.data <- temp, acs.data <- bind_rows(acs.data, temp))
    }
    
    for(i in 1:length(census)){
    acs.data <- acs.data %>%
      mutate(variable = case_when(variable == census[i] ~ cen.names[i],
                                  TRUE ~ variable))
    }
    
    acs.data <- acs.data %>%
      select(-c("NAME", "moe")) %>%
      st_drop_geometry()

#Crosswalk 2010 to 2020 for ACS data from 2015 to 2019
  acs.2015 <- acs.data %>%
    filter(year < 2020)

 acs.cross <- read.csv(file = "./Build/Input/nhgis_tr2010_tr2020_29.csv") %>%
   mutate(tr2010ge = as.character(tr2010ge),
          tr2020ge = as.character(tr2020ge)) %>%
   select(-c(tr2010gj, tr2020gj, parea, wt_adult, wt_fam, wt_hu, wt_ownhu, wt_renthu))
 
 hh_meas <- c("Tot_House", "female_HH", "nonfam", "other_fam")
 
 acs.temp <- acs.2015 %>%
   left_join(., acs.cross, c("GEOID" = "tr2010ge"), relationship = "many-to-many") %>%
   mutate(estimate = case_when(variable %in% hh_meas ~ estimate * wt_hh,
                               TRUE ~ estimate * wt_hh)) %>%
   select(-c("wt_pop", "GEOID", "wt_hh")) %>%
   rename("GEOID" = "tr2020ge") %>%
   aggregate(estimate ~ GEOID + year + variable, FUN = sum)
 
 acs.2020 <- acs.data %>%
   filter(year > 2019) %>%
   bind_rows(., acs.temp)
  
#Process the cross walked census data
 #Need area of 2020 census tracts

    temp <- temp %>%
      st_transform(., crs = st_crs(parcel)) %>%
      mutate(area = as.numeric(st_area(.))) %>%
      st_drop_geometry() %>%
      select(GEOID, area) %>%
      mutate(area = area / 27878400) %>% #convert to square mile
      distinct()
  
  #Process the cross walked acs data into usable work
    acs <- acs.2020 %>%
      left_join(., temp, by = "GEOID", relationship = "many-to-many") %>%
      distinct() %>%
      select(GEOID, variable, estimate, year, area) %>%
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
             per_fhh = female_HH / Tot_House,
             per_ohh = other_fam / Tot_House,
             per_nfhh = nonfam / Tot_House,
             dens = Tot_Pop / area) %>%
      select(GEOID, year, "Tot_Pop", "Med_Inc", area, dens, starts_with("per")) %>%
      filter(!is.na(area))

save(acs, file = "./Build/Output/census.RData")
  
    
