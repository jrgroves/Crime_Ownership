#This compiles the tract and grid level data for analysis and generates summary statistics.

#Jeremy R. Groves
#Created on: March 3, 2026

#UPDATED: March 22, 2026 - Muritala
#  1. Added Tot_Pop join to core.grid (missing from original build)
#  2. Added rate = event / (Tot_Pop / 1000) to core.grid
#     NOTE: Grid cell rates use tract residential population as denominator
#     via GEOID-year crosswalk since sub-tract population estimates unavailable
#  3. Added log_rate = log(rate + 1) — primary outcome in spatial models
#  4. Added rate_wins = pmin(rate, 99th percentile) — for map display only
#  5. Added grid_sf and tracts_sf geometry objects — filtered to core.grid cells
#     IMPORTANT: grid_sf is built here (not in 03_Grids) so it uses the correct
#     3,205 cells present in core.grid after filter(!is.na(owner))

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
    mutate(rate = event / (Tot_Pop / 1000)) %>%
    filter(!is.na(event))

  core.grid <- crime.grid %>%
    left_join(., tract.grid, by = "grid_id") %>%
    distinct() %>%
    left_join(., own.grid, by = c("grid_id", "year")) %>%
    distinct() %>%
    filter(!is.na(owner)) #Removes 4211 grid cells with no parcel data.

# =============================================================================
# ADDED: Compute rate, log_rate, and rate_wins for core.grid
# =============================================================================

  # Add tract population via GEOID-year crosswalk
  core.grid <- core.grid %>%
    left_join(
      acs %>% select(GEOID, year, Tot_Pop),
      by = c("GEOID", "year")
    ) %>%
    mutate(
      rate      = event / (Tot_Pop / 1000),          # crime rate per 1,000 tract residents
      log_rate  = log(rate + 1),                      # log rate — primary outcome in spatial models
      rate_wins = pmin(rate, quantile(rate, 0.99,     # winsorized rate — map display only
                                     na.rm = TRUE))
    )

# =============================================================================
# ADDED: Build grid_sf and tracts_sf geometry objects
# grid_sf filtered to core.grid cells (3,205) — To be done after core.grid
# is built so filter(!is.na(owner)) has already been applied
# =============================================================================

  # Reconstruct full grid in parcel CRS
  parcel_tmp <- read_sf("./Build/Input/Map/Parcels_Current.shp")
  map_tmp    <- read_sf("./Build/Input/Map/County_Bdy.shp") %>%
    st_transform(st_crs(parcel_tmp))

  grid_full <- st_make_grid(map_tmp, n = 100) %>%
    st_sf() %>%
    mutate(grid_id = 1:nrow(.))

  # Filter to cells present in core.grid and transform to WGS84
  grid_sf <- grid_full %>%
    filter(grid_id %in% unique(core.grid$grid_id)) %>%
    st_transform(crs = 4326) %>%
    select(grid_id, geometry)

  # Census tract geometries in WGS84
  tracts_sf <- tract.map %>%
    st_transform(crs = 4326)

  cat("grid_sf rows:", nrow(grid_sf), "\n")        # expect 3205
  cat("grid_sf ID range:", range(grid_sf$grid_id), "\n")  # expect 365 9472
  cat("tracts_sf rows:", nrow(tracts_sf), "\n")    # expect 236

  save(grid_sf, tracts_sf, file = "./Build/Output/grid_sf.RData")
  cat("grid_sf.RData saved.\n")

# =============================================================================
# Resave augmented core objects
# =============================================================================

  save(core.tract, core.grid, file = "./Build/Output/core_panels_augmented.RData")
  cat("core_panels_augmented.RData saved.\n")

# =============================================================================
# Summary statistics (original code)
# =============================================================================

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

