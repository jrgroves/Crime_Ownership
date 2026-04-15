# Corporate Ownership & Crime — Spatial Econometric Models
# Study area: St. Louis County, MO
# Scales: Census tract + Grid cell
# Outcome: ln(crime rate + 1)
# =============================================================================
# Authors: Jeremy Groves, Muritala Ogunsiji
# Updated: March 2026
# =============================================================================
# MODEL STRUCTURE:
#   A. Cross-sectional SAR (2024) — descriptive, between-unit identification
#   B. Spatial panel SAR (2018-2024) — within-tract identification
#      B1. Two-way FE (PREFERRED)
#          Rationale: year FE absorbs common temporal shocks (COVID-19 pandemic,
#          post-pandemic crime surge). AIC favors two-way over individual FE
#          (delta AIC = 308 personal, 225 property). Rho plausible (0.20-0.32)
#          vs individual FE where temporal autocorrelation (r=0.875) inflates
#          rho to 0.73.
#      B2. Individual FE (robustness)
#   C. Robustness checks (two-way FE)
#      C1. SDM panel (two-way FE, k=4) — Wlag.corporate insignificant,
#          confirms SAR correctly specified
#      C2. SAR panel (two-way FE, k=5) — confirms robustness to
#          weights choice
#
# SPATIAL WEIGHTS:
#   - Moran's I (in 01_EDA_Maps.R): queen contiguity (tracts), k-NN k=5 (grid)
#   - SAR models: k-NN k=4 for all scales (tracts and grid CS; tract panel)
#     Rationale: ensures fully connected weight matrices at all scales;
#     queen contiguity produces 2 subgraphs in property crime CS and
#     29 subgraphs in grid due to missing parcel cells
#   - Robustness: k-NN k=5 for tract panel
#
# KEY DECISIONS:
#   - SAR selected over SEM: LM diagnostic tests (adjRSlag dominant)
#   - ln(rate+1) outcome throughout
#   - Panel: 149 balanced tracts, 2018-2024 (pspatreg requirement)
# =============================================================================

library(tidyverse)
library(sf)
library(spdep)
library(spatialreg)
library(pspatreg)
library(car)

setwd("C:/Users/murta/OneDrive - Northern Illinois University/Desktop/NIU/Thesis/Ownership_Crime/Github/Crime_Ownership")

# =============================================================================
# 1. LOAD DATA
# =============================================================================

load("./Build/Output/core_panels_augmented.RData")
load("./Build/Output/census.RData")
load("./Build/Output/grid_sf.RData")
load("./Build/Output/eda_results.RData")

core.tract <- core.tract %>% mutate(log_rate = log(rate + 1))

# =============================================================================
# 2. FORMULAS
# =============================================================================

acs_controls_red <- c("Med_Inc", "dens", "per_male", "per_u18",
                       "per_blk", "per_pov1", "per_fhh")

f_tract_red <- as.formula(paste("log_rate ~ corporate +",
                                 paste(acs_controls_red, collapse = " + ")))

# Grid formula — tract FE + parcel-level controls
# luc.com excluded (perfectly collinear with luc.ag + luc.res)
f_grid <- as.formula("log_rate ~ corporate + owner + luc.ag + luc.res +
                       factor(GEOID)")

# =============================================================================
# 3. MULTICOLLINEARITY CHECK
# =============================================================================

cs_tmp <- core.tract %>%
  filter(year == 2024, OffenseCategory == "Person",
         !is.na(log_rate), !is.na(corporate)) %>%
  left_join(tracts_sf, by = "GEOID") %>% st_as_sf()

cat("VIF — reduced formula:\n")
print(vif(lm(f_tract_red, data = cs_tmp)))
cat("\nCorrelation per_wht ~ per_blk:",
    round(cor(cs_tmp$per_wht, cs_tmp$per_blk, use = "complete.obs"), 4), "\n")
rm(cs_tmp)

# =============================================================================
# 4. CROSS-SECTIONAL SAR (2024)
# =============================================================================

cs_tract_pers <- core.tract %>%
  filter(year == 2024, OffenseCategory == "Person",
         !is.na(log_rate), !is.na(corporate)) %>%
  left_join(tracts_sf, by = "GEOID") %>% st_as_sf()

cs_tract_prop <- core.tract %>%
  filter(year == 2024, OffenseCategory == "Property",
         !is.na(log_rate), !is.na(corporate)) %>%
  left_join(tracts_sf, by = "GEOID") %>% st_as_sf()

cs_grid_pers <- core.grid %>%
  filter(year == 2024, OffenseCategory == "Person",
         !is.na(log_rate), !is.na(corporate)) %>%
  left_join(grid_sf, by = "grid_id") %>% st_as_sf()

cs_grid_prop <- core.grid %>%
  filter(year == 2024, OffenseCategory == "Property",
         !is.na(log_rate), !is.na(corporate)) %>%
  left_join(grid_sf, by = "grid_id") %>% st_as_sf()

# Spatial weights — k-NN k=4 for all SAR models
# Tract CS
coords_tp         <- st_centroid(cs_tract_pers) %>% st_coordinates()
tract_knn_nb_pers <- knearneigh(coords_tp, k=4) %>% knn2nb()
tract_lw_cs       <- nb2listw(tract_knn_nb_pers, style="W")

coords_tr         <- st_centroid(cs_tract_prop) %>% st_coordinates()
tract_knn_nb_prop <- knearneigh(coords_tr, k=4) %>% knn2nb()
tract_lw_prop     <- nb2listw(tract_knn_nb_prop, style="W")

# Grid CS — k-NN k=4
coords_gp        <- st_centroid(cs_grid_pers) %>% st_coordinates()
grid_knn_nb_cs   <- knearneigh(coords_gp, k=4) %>% knn2nb()
grid_knn_lw_cs   <- nb2listw(grid_knn_nb_cs, style="W", zero.policy=TRUE)

coords_gpr       <- st_centroid(cs_grid_prop) %>% st_coordinates()
grid_knn_nb_prop <- knearneigh(coords_gpr, k=4) %>% knn2nb()
grid_knn_lw_prop <- nb2listw(grid_knn_nb_prop, style="W", zero.policy=TRUE)

# Verify all connected
cat("\nSubgraph check (all should be 1):\n")
cat("Tract pers:", n.comp.nb(tract_knn_nb_pers)$nc, "\n")
cat("Tract prop:", n.comp.nb(tract_knn_nb_prop)$nc, "\n")
cat("Grid pers: ", n.comp.nb(grid_knn_nb_cs)$nc,   "\n")
cat("Grid prop: ", n.comp.nb(grid_knn_nb_prop)$nc,  "\n")

# LM diagnostic tests
run_lm_tests <- function(formula, data, lw, label) {
  ols <- lm(formula, data = data)
  lmt <- lm.RStests(ols, lw,
                    test        = c("RSlag", "RSerr", "adjRSlag", "adjRSerr"),
                    zero.policy = TRUE)
  cat(sprintf("\n===== LM Tests: %s =====\n", label))
  print(summary(lmt))
  invisible(list(ols = ols, lmt = lmt))
}

lm_tract_pers <- run_lm_tests(f_tract_red, cs_tract_pers, tract_lw_cs,      "Tract Personal")
lm_tract_prop <- run_lm_tests(f_tract_red, cs_tract_prop, tract_lw_prop,    "Tract Property")
lm_grid_pers  <- run_lm_tests(f_grid,      cs_grid_pers,  grid_knn_lw_cs,   "Grid Personal")
lm_grid_prop  <- run_lm_tests(f_grid,      cs_grid_prop,  grid_knn_lw_prop, "Grid Property")

# Cross-sectional SAR
sar_cs_tract_pers <- lagsarlm(f_tract_red, data = cs_tract_pers,
                               listw = tract_lw_cs,   zero.policy = TRUE)
sar_cs_tract_prop <- lagsarlm(f_tract_red, data = cs_tract_prop,
                               listw = tract_lw_prop, zero.policy = TRUE)
sar_cs_grid_pers  <- lagsarlm(f_grid, data = cs_grid_pers,
                               listw = grid_knn_lw_cs,   zero.policy = TRUE)
sar_cs_grid_prop  <- lagsarlm(f_grid, data = cs_grid_prop,
                               listw = grid_knn_lw_prop, zero.policy = TRUE)

cat("\n===== Cross-Sectional SAR Summary =====\n")
models <- list(sar_cs_tract_pers, sar_cs_tract_prop,
               sar_cs_grid_pers,  sar_cs_grid_prop)
lws_cs <- list(tract_lw_cs, tract_lw_prop, grid_knn_lw_cs, grid_knn_lw_prop)
labels <- c("Tract Personal", "Tract Property", "Grid Personal", "Grid Property")

for(i in 1:4) {
  mt <- moran.test(residuals(models[[i]]), lws_cs[[i]], zero.policy = TRUE)
  cat(sprintf("%s — Corporate: %.4f | Rho: %.4f | Resid AC p: %.4f\n",
              labels[i], coef(models[[i]])["corporate"],
              models[[i]]$rho, mt$p.value))
}

# Direct/Indirect/Total effects
imp_cs_tract_pers <- impacts(sar_cs_tract_pers, listw = tract_lw_cs,   R = 999)
imp_cs_tract_prop <- impacts(sar_cs_tract_prop, listw = tract_lw_prop, R = 999)

cat("\n===== Impacts: Tract Personal (cross-section) =====\n")
summary(imp_cs_tract_pers, zstats = TRUE, short = TRUE)
cat("\n===== Impacts: Tract Property (cross-section) =====\n")
summary(imp_cs_tract_prop, zstats = TRUE, short = TRUE)

# =============================================================================
# 5. SPATIAL PANEL SAR (2018-2024, 149 balanced tracts)
# =============================================================================

balanced_geoids <- core.tract %>%
  filter(OffenseCategory == "Person", year >= 2018) %>%
  group_by(GEOID) %>%
  summarise(n_years = n_distinct(year)) %>%
  filter(n_years == 7) %>%
  pull(GEOID)

panel_tract_pers <- core.tract %>%
  filter(OffenseCategory == "Person", year >= 2018,
         GEOID %in% balanced_geoids,
         !is.na(log_rate), !is.na(corporate)) %>%
  st_drop_geometry() %>% as.data.frame() %>% arrange(GEOID, year)

panel_tract_prop <- core.tract %>%
  filter(OffenseCategory == "Property", year >= 2018,
         GEOID %in% balanced_geoids,
         !is.na(log_rate), !is.na(corporate)) %>%
  st_drop_geometry() %>% as.data.frame() %>% arrange(GEOID, year)

# Panel weights: k-NN k=4 (main), k=5 (robustness)
bal_tracts_sf <- tracts_sf %>% filter(GEOID %in% balanced_geoids) %>% arrange(GEOID)
cent_bal      <- st_centroid(bal_tracts_sf)

knn_bal <- knn2nb(knearneigh(cent_bal$geometry, k=4))
attr(knn_bal, "region.id") <- bal_tracts_sf$GEOID
lw_bal  <- nb2listw(knn_bal, style="W")

knn5_bal <- knn2nb(knearneigh(cent_bal$geometry, k=5))
attr(knn5_bal, "region.id") <- bal_tracts_sf$GEOID
lw_bal_k5 <- nb2listw(knn5_bal, style="W")

set.seed(123)

# =============================================================================
# B1. Two-way FE (PREFERRED)
# Rationale: absorbs common temporal shocks (COVID-19, post-pandemic crime
# surge). AIC favors two-way FE. Rho plausible (0.20-0.32) vs individual FE
# where temporal autocorrelation inflates rho to ~0.73.
# =============================================================================

sar_panel_pers_2way <- pspatfit(f_tract_red, data = panel_tract_pers,
                                 listw = lw_bal, demean = TRUE,
                                 eff_demean = "twoways", type = "sar",
                                 index = c("GEOID", "year"))

sar_panel_prop_2way <- pspatfit(f_tract_red, data = panel_tract_prop,
                                 listw = lw_bal, demean = TRUE,
                                 eff_demean = "twoways", type = "sar",
                                 index = c("GEOID", "year"))

cat("\n===== SAR Panel: Personal (two-way FE — PREFERRED) =====\n")
summary(sar_panel_pers_2way)
cat("\n===== SAR Panel: Property (two-way FE — PREFERRED) =====\n")
summary(sar_panel_prop_2way)

# Two-way FE impacts (PREFERRED)
imp_panel_pers_2way <- impactspar(sar_panel_pers_2way, listw = lw_bal)
imp_panel_prop_2way <- impactspar(sar_panel_prop_2way, listw = lw_bal)

cat("\n===== Impacts: Personal (panel, two-way FE) =====\n")
summary(imp_panel_pers_2way)
cat("\n===== Impacts: Property (panel, two-way FE) =====\n")
summary(imp_panel_prop_2way)

# =============================================================================
# B2. Individual FE (robustness)
# =============================================================================

sar_panel_pers_1way <- pspatfit(f_tract_red, data = panel_tract_pers,
                                 listw = lw_bal, demean = TRUE,
                                 eff_demean = "individual", type = "sar",
                                 index = c("GEOID", "year"))

sar_panel_prop_1way <- pspatfit(f_tract_red, data = panel_tract_prop,
                                 listw = lw_bal, demean = TRUE,
                                 eff_demean = "individual", type = "sar",
                                 index = c("GEOID", "year"))

cat("\n===== SAR Panel: Personal (individual FE — robustness) =====\n")
summary(sar_panel_pers_1way)
cat("\n===== SAR Panel: Property (individual FE — robustness) =====\n")
summary(sar_panel_prop_1way)

# Individual FE impacts (robustness)
imp_panel_pers <- impactspar(sar_panel_pers_1way, listw = lw_bal)
imp_panel_prop <- impactspar(sar_panel_prop_1way, listw = lw_bal)

cat("\n===== Impacts: Personal (panel, individual FE) =====\n")
summary(imp_panel_pers)
cat("\n===== Impacts: Property (panel, individual FE) =====\n")
summary(imp_panel_prop)

# AIC comparison
cat("\n===== AIC: Individual vs Two-way FE =====\n")
cat(sprintf("Individual FE — Personal: %.1f | Property: %.1f\n",
            sar_panel_pers_1way$aic, sar_panel_prop_1way$aic))
cat(sprintf("Two-way FE   — Personal: %.1f | Property: %.1f\n",
            sar_panel_pers_2way$aic, sar_panel_prop_2way$aic))
cat(sprintf("Delta AIC    — Personal: %.1f | Property: %.1f\n",
            sar_panel_pers_1way$aic - sar_panel_pers_2way$aic,
            sar_panel_prop_1way$aic - sar_panel_prop_2way$aic))

# =============================================================================
# 6. ROBUSTNESS CHECKS (two-way FE)
# =============================================================================

# C1. SDM panel (two-way FE, k=4)
sdm_panel_pers <- pspatfit(f_tract_red, data = panel_tract_pers,
                            listw = lw_bal, demean = TRUE,
                            eff_demean = "twoways", type = "sdm",
                            index = c("GEOID", "year"))

sdm_panel_prop <- pspatfit(f_tract_red, data = panel_tract_prop,
                            listw = lw_bal, demean = TRUE,
                            eff_demean = "twoways", type = "sdm",
                            index = c("GEOID", "year"))

cat("\n===== SDM Panel: Personal (two-way FE) =====\n")
summary(sdm_panel_pers)
cat("\n===== SDM Panel: Property (two-way FE) =====\n")
summary(sdm_panel_prop)

# SDM impacts
imp_sdm_pers <- impactspar(sdm_panel_pers, listw = lw_bal)
imp_sdm_prop <- impactspar(sdm_panel_prop, listw = lw_bal)

# C2. SAR panel k=5 (two-way FE)
sar_panel_pers_k5 <- pspatfit(f_tract_red, data = panel_tract_pers,
                               listw = lw_bal_k5, demean = TRUE,
                               eff_demean = "twoways", type = "sar",
                               index = c("GEOID", "year"))

sar_panel_prop_k5 <- pspatfit(f_tract_red, data = panel_tract_prop,
                               listw = lw_bal_k5, demean = TRUE,
                               eff_demean = "twoways", type = "sar",
                               index = c("GEOID", "year"))

cat("\n===== SAR Panel k=5: Personal (two-way FE) =====\n")
summary(sar_panel_pers_k5)
cat("\n===== SAR Panel k=5: Property (two-way FE) =====\n")
summary(sar_panel_prop_k5)

# SAR k=5 impacts
imp_k5_pers <- impactspar(sar_panel_pers_k5, listw = lw_bal_k5)
imp_k5_prop <- impactspar(sar_panel_prop_k5, listw = lw_bal_k5)

cat("\n===== AIC COMPARISON =====\n")
cat(sprintf("Two-way SAR k=4  Personal: %.1f | Property: %.1f\n",
            sar_panel_pers_2way$aic, sar_panel_prop_2way$aic))
cat(sprintf("Two-way SDM k=4  Personal: %.1f | Property: %.1f\n",
            sdm_panel_pers$aic,      sdm_panel_prop$aic))
cat(sprintf("Two-way SAR k=5  Personal: %.1f | Property: %.1f\n",
            sar_panel_pers_k5$aic,   sar_panel_prop_k5$aic))

# =============================================================================
# 7. SAVE
# =============================================================================

save(sar_cs_tract_pers, sar_cs_tract_prop,
     sar_cs_grid_pers,  sar_cs_grid_prop,
     imp_cs_tract_pers, imp_cs_tract_prop,
     sar_panel_pers_2way, sar_panel_prop_2way,   # PREFERRED
     imp_panel_pers_2way, imp_panel_prop_2way,   # PREFERRED impacts
     sar_panel_pers_1way, sar_panel_prop_1way,   # robustness
     imp_panel_pers,      imp_panel_prop,         # robustness impacts
     sdm_panel_pers,      sdm_panel_prop,
     imp_sdm_pers,        imp_sdm_prop,
     sar_panel_pers_k5,   sar_panel_prop_k5,
     imp_k5_pers,         imp_k5_prop,
     tract_lw_cs, tract_lw_prop,
     grid_knn_lw_cs, grid_knn_lw_prop,
     lw_bal, lw_bal_k5, balanced_geoids,
     panel_tract_pers, panel_tract_prop,
     cs_tract_pers, cs_tract_prop,
     cs_grid_pers,  cs_grid_prop,
     f_tract_red, f_grid, acs_controls_red,
     file = "./Build/Output/spatial_model_results.RData")

cat("\n02_Spatial_Models.R complete.\n")
