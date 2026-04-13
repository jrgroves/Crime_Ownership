# =============================================================================
# 01_EDA_Maps.R
# Corporate Ownership & Crime — Exploratory Spatial Analysis
# Study area: St. Louis County, MO (FIPS: 29189)
# Scales: Census tract + Grid cell (100x100 fishnet)
# Outcomes: Personal crime rate, property crime rate (per 1,000)
# =============================================================================
# Authors: Jeremy Groves, Muritala Ogunsiji
# Updated: March 2026
# =============================================================================
# NOTES:
#   - OffenseCategory values: "Person" and "Property"
#   - corporate: ownership share (0-1) — treatment variable at both scales
#   - owner: owner-occupancy rate (0-1) — tenure CONTROL, not treatment
#   - Grid geometry: reconstructed via st_make_grid(map, n=100) in parcel CRS
#   - Grid weights: k-NN (k=5); queen contiguity produces 29 subgraphs
#   - rate_wins: winsorized at 99th percentile for grid display only
#   - tmap version: 4.2 — uses tm_scale_intervals() not style="quantile"
# =============================================================================
rm(list=ls())

library(tidyverse)
library(sf)
library(spdep)
library(tmap)

#setwd("C:/Users/murta/OneDrive - Northern Illinois University/Desktop/NIU/Thesis/Ownership_Crime/Github/Crime_Ownership")

tmap_mode("plot")
tmap_options(component.autoscale = FALSE)

# =============================================================================
# 1. LOAD DATA
# =============================================================================

load("./Build/Output/core_panels_augmented.RData")
load("./Build/Output/census.RData")
load("./Build/Output/grid_sf.RData")

core.tract <- core.tract %>% mutate(log_rate = log(rate + 1))

cat("Data loaded.\n")
cat("Tract obs:", nrow(core.tract), "| Grid obs:", nrow(core.grid), "\n")
cat("Years:", sort(unique(core.tract$year)), "\n")
cat("OffenseCategory:", unique(core.tract$OffenseCategory), "\n")

# =============================================================================
# 2. BUILD MAP OBJECTS (2024)
# =============================================================================

map_year <- 2024

tract_map <- core.tract %>%
  filter(year == map_year) %>%
  pivot_wider(names_from  = OffenseCategory,
              values_from = c(rate, log_rate, event),
              names_sep   = "_") %>%
  right_join(tracts_sf, by = "GEOID") %>%
  st_as_sf()

grid_map <- core.grid %>%
  filter(year == map_year) %>%
  pivot_wider(names_from  = OffenseCategory,
              values_from = c(rate, log_rate, rate_wins, event),
              names_sep   = "_") %>%
  right_join(grid_sf, by = "grid_id") %>%
  st_as_sf()

cat("Map objects built.\n")

# =============================================================================
# 3. CHOROPLETH MAPS
# =============================================================================

qmap <- function(sf_obj, var, title, borders = TRUE) {
  m <- tm_shape(sf_obj) +
    tm_fill(var,
            fill.scale  = tm_scale_intervals(style  = "quantile", n = 5,
                                             values = "brewer.yl_or_rd"),
            fill.legend = tm_legend(title = "Quintiles")) +
    tm_title(title)
  if (borders) m <- m + tm_borders(lwd = 0.3, col = "white")
  m
}

tmap_arrange(
  qmap(tract_map, "corporate",    "(a) Corporate ownership %"),
  qmap(tract_map, "rate_Person",  "(b) Personal crime rate"),
  qmap(tract_map, "rate_Property","(c) Property crime rate"),
  ncol = 3
)

tmap_arrange(
  qmap(grid_map, "corporate",          "(a) Corporate ownership %",  borders=FALSE),
  qmap(grid_map, "rate_wins_Person",   "(b) Personal crime rate",    borders=FALSE),
  qmap(grid_map, "rate_wins_Property", "(c) Property crime rate",    borders=FALSE),
  ncol = 3
)

# =============================================================================
# 4. SPATIAL WEIGHTS
# =============================================================================

tract_nb <- poly2nb(tract_map, queen = TRUE)
tract_lw <- nb2listw(tract_nb, style = "W", zero.policy = TRUE)

coords_grid <- st_centroid(grid_map) %>% st_coordinates()
grid_knn_nb <- knearneigh(coords_grid, k = 5) %>% knn2nb()
grid_knn_lw <- nb2listw(grid_knn_nb, style = "W", zero.policy = TRUE)

cat("\nTract subgraphs:", n.comp.nb(tract_nb)$nc, "\n")
cat("Grid subgraphs (k-NN k=5):", n.comp.nb(grid_knn_nb)$nc, "\n")

# =============================================================================
# 5. GLOBAL MORAN'S I
# =============================================================================

run_moran <- function(sf_obj, lw, var, label) {
  x      <- sf_obj[[var]]
  valid  <- !is.na(x)
  lw_sub <- subset(lw, valid, zero.policy = TRUE)
  mt     <- moran.test(x[valid], lw_sub, zero.policy = TRUE)
  cat(sprintf("Moran's I [%s — %s]: I = %.4f, p = %.4e\n",
              label, var, mt$estimate[1], mt$p.value))
  invisible(mt)
}

cat("\n===== GLOBAL MORAN'S I =====\n")
mt_tract_pers <- run_moran(tract_map, tract_lw,    "rate_Person",        "Tract")
mt_tract_prop <- run_moran(tract_map, tract_lw,    "rate_Property",      "Tract")
mt_tract_own  <- run_moran(tract_map, tract_lw,    "corporate",          "Tract")
mt_grid_pers  <- run_moran(grid_map,  grid_knn_lw, "rate_wins_Person",   "Grid")
mt_grid_prop  <- run_moran(grid_map,  grid_knn_lw, "rate_wins_Property", "Grid")
mt_grid_own   <- run_moran(grid_map,  grid_knn_lw, "corporate",          "Grid")

# =============================================================================
# 6. LISA
# =============================================================================

compute_lisa <- function(sf_obj, lw, var) {
  x      <- sf_obj[[var]]
  valid  <- !is.na(x)
  lw_sub <- subset(lw, valid, zero.policy = TRUE)
  lisa   <- localmoran(x[valid], lw_sub,
                       zero.policy = TRUE, alternative = "two.sided")
  x_std   <- scale(x[valid])[, 1]
  lag_std <- lag.listw(lw_sub, x_std, zero.policy = TRUE)
  quadrant <- case_when(
    lisa[, 5] < 0.05 & x_std > 0 & lag_std > 0 ~ "High-High",
    lisa[, 5] < 0.05 & x_std < 0 & lag_std < 0 ~ "Low-Low",
    lisa[, 5] < 0.05 & x_std > 0 & lag_std < 0 ~ "High-Low",
    lisa[, 5] < 0.05 & x_std < 0 & lag_std > 0 ~ "Low-High",
    TRUE ~ "Not significant"
  )
  out <- sf_obj[valid, ]
  out$lisa_quad <- quadrant
  out$lisa_I    <- lisa[, 1]
  out$lisa_p    <- lisa[, 5]
  out
}

cat("\nComputing LISA...\n")
lisa_tract_pers <- compute_lisa(tract_map, tract_lw,    "rate_Person")
lisa_tract_prop <- compute_lisa(tract_map, tract_lw,    "rate_Property")
lisa_tract_own  <- compute_lisa(tract_map, tract_lw,    "corporate")
lisa_grid_pers  <- compute_lisa(grid_map,  grid_knn_lw, "rate_wins_Person")
lisa_grid_prop  <- compute_lisa(grid_map,  grid_knn_lw, "rate_wins_Property")
lisa_grid_own   <- compute_lisa(grid_map,  grid_knn_lw, "corporate")

# Distributions
cat("\nTract personal crime LISA quads:\n"); print(table(lisa_tract_pers$lisa_quad))
cat("\nTract property crime LISA quads:\n"); print(table(lisa_tract_prop$lisa_quad))
cat("\nTract corporate ownership LISA quads:\n"); print(table(lisa_tract_own$lisa_quad))
cat("\nGrid personal crime LISA quads:\n");  print(table(lisa_grid_pers$lisa_quad))
cat("\nGrid property crime LISA quads:\n");  print(table(lisa_grid_prop$lisa_quad))
cat("\nGrid corporate ownership LISA quads:\n"); print(table(lisa_grid_own$lisa_quad))

# Hotspot overlap
hh_pers <- lisa_tract_pers$GEOID[lisa_tract_pers$lisa_quad == "High-High"]
hh_prop <- lisa_tract_prop$GEOID[lisa_tract_prop$lisa_quad == "High-High"]
hh_own  <- lisa_tract_own$GEOID[lisa_tract_own$lisa_quad   == "High-High"]

cat(sprintf("\nPersonal crime HH overlap with ownership HH: %d of %d (%.1f%%)\n",
            sum(hh_pers %in% hh_own), length(hh_pers),
            100 * mean(hh_pers %in% hh_own)))
cat(sprintf("Property crime HH overlap with ownership HH: %d of %d (%.1f%%)\n",
            sum(hh_prop %in% hh_own), length(hh_prop),
            100 * mean(hh_prop %in% hh_own)))

# LISA maps
lisa_pal <- c("High-High"       = "#E24B4A",
              "Low-Low"         = "#378ADD",
              "High-Low"        = "#EF9F27",
              "Low-High"        = "#9FE1CB",
              "Not significant" = "#D3D1C7")

lmap <- function(sf_obj, title, borders = TRUE) {
  m <- tm_shape(sf_obj) +
    tm_fill("lisa_quad",
            fill.scale  = tm_scale_categorical(values = lisa_pal),
            fill.legend = tm_legend(title = "Cluster type")) +
    tm_title(title)
  if (borders) m <- m + tm_borders(lwd = 0.3, col = "white")
  m
}

tmap_arrange(
  lmap(lisa_tract_pers, "(a) Personal crime clusters"),
  lmap(lisa_tract_prop, "(b) Property crime clusters"),
  lmap(lisa_tract_own,  "(c) Corporate ownership clusters"),
  ncol = 3
)

tmap_arrange(
  lmap(lisa_grid_pers, "(a) Personal crime clusters",      borders=FALSE),
  lmap(lisa_grid_prop, "(b) Property crime clusters",      borders=FALSE),
  lmap(lisa_grid_own,  "(c) Corporate ownership clusters", borders=FALSE),
  ncol = 3
)

# =============================================================================
# 8. SAVE
# =============================================================================

save(tract_map, grid_map,
     tract_nb, tract_lw, grid_knn_nb, grid_knn_lw,
     lisa_tract_pers, lisa_tract_prop, lisa_tract_own,
     lisa_grid_pers,  lisa_grid_prop,  lisa_grid_own,
     mt_tract_pers, mt_tract_prop, mt_tract_own,
     mt_grid_pers,  mt_grid_prop,  mt_grid_own,
     file = "./Build/Output/eda_results.RData")

cat("\n01_EDA_Maps.R complete.\n")
cat("Saved to ./Build/Output/eda_results.RData\n")
cat("Proceed to 02_Spatial_Models.R\n")
