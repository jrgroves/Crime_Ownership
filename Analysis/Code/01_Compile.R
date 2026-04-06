#This compiles the tract and grid level data for analysis and generates summary statistics.

#Jeremy R. Groves
#Created on: March 3, 2026

rm(list=ls())

library(sf)
library(tidyverse)
library(gtsummary)

#Load Data

load("./Build/Output/tractagg.RData")
load("./Build/Output/MO_Crime_Prop.RData")
load("./Build/Output/census.RData")
load("./Build/Output/Grid_data.RData")

#Combine census and crime datasets

  core.tract <- acs %>%
    left_join(., tract.agg, by = c("GEOID", "year")) %>%
    mutate(rate = event/(Tot_Pop/1000)) %>%
    filter(!is.na(event))
  
  core.grid <- crime.grid %>%
    left_join(., tract.grid, by = "grid_id") %>%
    distinct() %>%
    left_join(., own.grid, by = c("grid_id", "year")) %>%
    distinct() %>%
    filter(!is.na(owner)) #Removes 4211 grid cells with no parcel data.
  
