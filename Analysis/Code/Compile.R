#This compiles the tract information for tract level analysis

#Jeremy R. Groves
#Created on: March 3, 2026

rm(list=ls())

library(sf)
library(tidyverse)
library(gtsummary)

#Load Data

load("./Build/Output/MO_Crime_Prop.RData")
load("./Build/Output/census.RData")

#Combine census and crime datasets

  #Remove NA offense category from tract.crime befor join
    temp <- tract.crime %>%
      filter(!is.na("OffenseCategory")) %>%
      rename("Offense" = "OffenseCategory") 

core <- acs %>%
  left_join(., temp, by = c("GEOID", "year")) %>%
  mutate(rate = event/(Tot_Pop/1000),
         Offense = replace_na(Offense, "Missing")) %>%
  filter(!is.na(Med_Inc))

sum.data <- core %>%
  select(-c(GEOID)) %>%
  mutate(area = as.vector(area),
         dens = as.vector(dens))%>%
  filter(Offense != "Missing",
         Offense != "N/A") %>%
  tbl_summary(by = year)

