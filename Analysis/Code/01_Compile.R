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
  

sum.dat1 <- core.tract %>%
  select(-c(GEOID, parcel, area)) %>%
  relocate(rate, .before = year) %>%
  relocate(c(Tot_Pop, Med_Inc), .after = last_col()) %>%
  tbl_strata(
    strata = OffenseCategory,
    .tbl_fun = 
      ~.x %>%
      tbl_summary(by = year,
                  digits = list(all_continuous() ~ c(4,4),
                                all_categorical() ~ c(2,0)),
                  statistic = list(all_continuous() ~ "{mean} ({sd})",
                                   all_categorical() ~ "{p}% {n}")),
    .header = "**{strata}**, N = {n}")

sum.dat2 <- core.grid %>%
  select(-c(grid_id, GEOID)) %>%
  tbl_strata(
    strata = OffenseCategory,
    .tbl_fun = 
      ~.x %>%
      tbl_summary(by = year,
                  digits = list(all_continuous() ~ c(4,4),
                                all_categorical() ~ c(2,0)),
                  statistic = list(all_continuous() ~ "{mean} ({sd})",
                                   all_categorical() ~ "{p}% {n}")),
    .header = "**{strata}**, N = {n}")



