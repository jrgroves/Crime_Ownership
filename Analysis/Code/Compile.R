#This compiles the tract information for tract level analysis

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

#Combine census and crime datasets

  core <- acs %>%
    left_join(., tract.agg, by = c("GEOID", "year")) %>%
    mutate(rate = event/(Tot_Pop/1000)) %>%
    filter(!is.na(event))

sum.data <- core %>%
  select(-c(GEOID)) %>%
  tbl_summary(by = year)

