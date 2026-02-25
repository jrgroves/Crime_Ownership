#Creates the census tract population and other census features

#By: Jeremy Groves
#Date: February 23. 2026

rm(list=ls())

library(tidyverse)
library(tidycensus)

#Load data and maps

years = seq(2020, 2024) #2025 data not available yet
census <- c("B01001_001")

for(i in years){
    temp <- get_acs(geography = "tract",
                    variables = census,
                    state = "29",
                    county = "189",
                    year = i,
                    geometry = FALSE
                       
    )
    temp$year <- i
    ifelse(i == 2020, acs.data <- temp, acs.data <- bind_rows(acs.data, temp))
}

acs <- acs.data %>%
  mutate(var = case_match(
    variable,
    "B01001_001" ~ "TotPop")) %>%
  select(GEOID, var, estimate, year) %>%
  pivot_wider(id_cols = c("GEOID", "year"), names_from = var, values_from = estimate)

save(acs, file = "./Build/Output/census.RData")
  
    
