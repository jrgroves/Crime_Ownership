# =============================================================================
# 03_Tables_Maps.R
# Corporate Ownership & Crime — Tables and Maps
#
# TO UPDATE: source("01_EDA_Maps.R") then source("02_Spatial_Models.R")
#            then source("03_Tables_Maps.R")
#
# OUTPUTS:
#   ./Analysis/Output/Tables/Spatial_Results_Tables.docx
#   ./Analysis/Output/Maps/Fig1-Fig5.png (300 DPI)
# =============================================================================

library(tidyverse)
library(sf)
library(spdep)
library(spatialreg)
library(officer)
library(flextable)
library(tmap)

#setwd("C:/Users/murta/OneDrive - Northern Illinois University/Desktop/NIU/Thesis/Ownership_Crime/Github/Crime_Ownership")

dir.create("./Analysis/Output/Tables", showWarnings = FALSE, recursive = TRUE)
dir.create("./Analysis/Output/Maps",   showWarnings = FALSE, recursive = TRUE)

# =============================================================================
# 1. LOAD
# =============================================================================

load("./Build/Output/core_panels_augmented.RData")
load("./Build/Output/census.RData")
load("./Build/Output/grid_sf.RData")
load("./Build/Output/eda_results.RData")
load("./Build/Output/spatial_model_results.RData")

core.tract <- core.tract %>% mutate(log_rate = log(rate + 1))

# =============================================================================
# 2. HELPERS
# =============================================================================

sig_stars <- function(p) {
  case_when(p < 0.001 ~ "***", p < 0.01 ~ "**",
            p < 0.05  ~ "*",   p < 0.1  ~ "\u2020", TRUE ~ "")
}

fmt_coef <- function(coef, se, stars) {
  coef_fmt <- ifelse(abs(coef) < 0.001,
                     formatC(coef, format = "e", digits = 2),
                     as.character(round(coef, 3)))
  se_fmt <- ifelse(abs(se) < 0.001,
                   formatC(se, format = "e", digits = 2),
                   as.character(round(se, 3)))
  paste0(coef_fmt, stars, " (", se_fmt, ")")
}

fmt_p <- function(p) ifelse(p < 0.001, "<0.001", as.character(round(p, 4)))

ft_theme <- function(ft) {
  ft %>%
    theme_booktabs() %>%
    font(fontname = "Times New Roman", part = "all") %>%
    fontsize(size = 10, part = "all") %>%
    bold(part = "header") %>%
    align(align = "center", part = "all") %>%
    align(j = 1, align = "left", part = "all") %>%
    padding(padding = 3, part = "all") %>%
    set_table_properties(width = 1, layout = "autofit")
}

# =============================================================================
# 3. TABLE 1: DESCRIPTIVE STATISTICS
# =============================================================================

make_table1 <- function() {
  desc_var <- function(x) {
    data.frame(N      = sum(!is.na(x)),
               Mean   = round(mean(x, na.rm=TRUE), 3),
               Median = round(median(x, na.rm=TRUE), 3),
               SD     = round(sd(x, na.rm=TRUE), 3),
               Min    = round(min(x, na.rm=TRUE), 3),
               Max    = round(max(x, na.rm=TRUE), 3))
  }
  tp <- core.tract %>% filter(OffenseCategory == "Person")
  tr <- core.tract %>% filter(OffenseCategory == "Property")
  gp <- core.grid  %>% filter(OffenseCategory == "Person")
  gr <- core.grid  %>% filter(OffenseCategory == "Property")

  vars_tract <- list(
    "Personal crime rate (per 1,000)" = tp$rate,
    "Property crime rate (per 1,000)" = tr$rate,
    "Corporate ownership %"           = tp$corporate,
    "Trustee %"                       = tp$trustee,
    "Nonprofit %"                     = tp$nonprofit,
    "REO %"                           = tp$reown,
    "Partnership %"                   = tp$partnership,
    "HOA %"                           = tp$hoa,
    "Municipal %"                     = tp$muni,
    "Non-owner occupancy %"           = tp$nonowner,
    "Median income ($)"               = tp$Med_Inc,
    "Population density"              = tp$dens,
    "% Male"                          = tp$per_male,
    "% Under 18"                      = tp$per_u18,
    "% White"                         = tp$per_wht,
    "% Black"                         = tp$per_blk,
    "% Below poverty line"            = tp$per_pov1,
    "% Female-headed households"      = tp$per_fhh
  )
  vars_grid <- list(
    "Personal crime rate (per 1,000)" = gp$rate,
    "Property crime rate (per 1,000)" = gr$rate,
    "Corporate ownership %"           = gp$corporate,
    "Owner-occupancy %"               = gp$owner,
    "% Agricultural land use"         = gp$luc.ag,
    "% Residential land use"          = gp$luc.res,
    "% Commercial land use"           = gp$luc.com
  )
  tract_df <- map_dfr(names(vars_tract), ~ {
    desc_var(vars_tract[[.x]]) %>% mutate(Variable = .x, Scale = "Tract")
  }) %>% select(Scale, Variable, N, Mean, Median, SD, Min, Max)

  grid_df <- map_dfr(names(vars_grid), ~ {
    desc_var(vars_grid[[.x]]) %>% mutate(Variable = .x, Scale = "Grid")
  }) %>% select(Scale, Variable, N, Mean, Median, SD, Min, Max)

  bind_rows(tract_df, grid_df)
}

# =============================================================================
# 4. TABLE 2: MORAN'S I
# =============================================================================

make_table2 <- function() {
  list(
    list("Personal crime rate",   "Tract", mt_tract_pers),
    list("Property crime rate",   "Tract", mt_tract_prop),
    list("Corporate ownership %", "Tract", mt_tract_own),
    list("Personal crime rate",   "Grid",  mt_grid_pers),
    list("Property crime rate",   "Grid",  mt_grid_prop),
    list("Corporate ownership %", "Grid",  mt_grid_own)
  ) %>%
    map_dfr(~ data.frame(
      Variable    = .x[[1]], Scale = .x[[2]],
      `Moran's I` = round(.x[[3]]$estimate[1], 4),
      `p-value`   = fmt_p(.x[[3]]$p.value),
      check.names = FALSE
    ))
}

# =============================================================================
# 5. TABLE 3: CROSS-SECTIONAL SAR
# =============================================================================

make_table3 <- function() {
  extract <- function(mod, lw, scale, crime) {
    cf    <- coef(mod); se <- sqrt(diag(vcov(mod)))
    pv    <- 2 * pnorm(-abs(cf / se))
    mt    <- moran.test(residuals(mod), lw, zero.policy = TRUE)
    rho_p <- 2 * pnorm(-abs(mod$rho / mod$rho.se))

    var_labels <- c(
      "(Intercept)" = "Intercept",
      "corporate"   = "Corporate ownership %",
      "owner"       = "Owner-occupancy %",
      "luc.ag"      = "% Agricultural land",
      "luc.res"     = "% Residential land",
      "Med_Inc"     = "Median income",
      "dens"        = "Population density",
      "per_male"    = "% Male",
      "per_u18"     = "% Under 18",
      "per_blk"     = "% Black",
      "per_pov1"    = "% Below poverty",
      "per_fhh"     = "% Female-headed HH"
    )
    rows <- map_dfr(names(cf), function(v) {
      lbl <- var_labels[v]
      if (is.na(lbl)) return(NULL)
      data.frame(Variable = lbl,
                 Estimate = fmt_coef(cf[v], se[v], sig_stars(pv[v])),
                 check.names = FALSE)
    })
    rows <- bind_rows(rows,
      data.frame(Variable = "Spatial lag (rho)",
                 Estimate = fmt_coef(mod$rho, mod$rho.se, sig_stars(rho_p))),
      data.frame(Variable = "N",
                 Estimate = as.character(length(mod$residuals))),
      data.frame(Variable = "AIC",
                 Estimate = as.character(round(AIC(mod), 1))),
      data.frame(Variable = "Residual AC p",
                 Estimate = fmt_p(mt$p.value))
    )
    names(rows)[2] <- paste0(scale, " \u2014 ", crime)
    rows
  }
  list(
    extract(sar_cs_tract_pers, tract_lw_cs,      "Tract", "Personal"),
    extract(sar_cs_tract_prop, tract_lw_prop,    "Tract", "Property"),
    extract(sar_cs_grid_pers,  grid_knn_lw_cs,   "Grid",  "Personal"),
    extract(sar_cs_grid_prop,  grid_knn_lw_prop, "Grid",  "Property")
  ) %>%
    reduce(full_join, by = "Variable") %>%
    mutate(across(everything(), ~ replace_na(as.character(.), "\u2014")))
}

# =============================================================================
# 6. TABLE 4: SPATIAL PANEL SAR
# =============================================================================

make_table4 <- function() {
  extract <- function(mod, label) {
    cf    <- mod$bfixed; se <- mod$se_bfixed
    pv    <- 2 * pnorm(-abs(cf / se))
    rho_p <- 2 * pnorm(-abs(mod$rho / mod$se_rho))

    var_labels <- c(
      "fixed_corporate" = "Corporate ownership %",
      "fixed_Med_Inc"   = "Median income",
      "fixed_dens"      = "Population density",
      "fixed_per_male"  = "% Male",
      "fixed_per_u18"   = "% Under 18",
      "fixed_per_blk"   = "% Black",
      "fixed_per_pov1"  = "% Below poverty",
      "fixed_per_fhh"   = "% Female-headed HH"
    )
    rows <- map_dfr(names(cf), function(v) {
      lbl <- var_labels[v]
      if (is.na(lbl)) return(NULL)
      data.frame(Variable = lbl,
                 Estimate = fmt_coef(cf[v], se[v], sig_stars(pv[v])),
                 check.names = FALSE)
    })
    rows <- bind_rows(rows,
      data.frame(Variable = "Spatial lag (rho)",
                 Estimate = fmt_coef(mod$rho, mod$se_rho, sig_stars(rho_p))),
      data.frame(Variable = "N",
                 Estimate = as.character(mod$nfull)),
      data.frame(Variable = "AIC",
                 Estimate = as.character(round(mod$aic, 1)))
    )
    names(rows)[2] <- label
    rows
  }
  list(
    extract(sar_panel_pers_1way, "Personal \u2014 Indiv. FE"),
    extract(sar_panel_prop_1way, "Property \u2014 Indiv. FE"),
    extract(sar_panel_pers_2way, "Personal \u2014 Two-way FE"),
    extract(sar_panel_prop_2way, "Property \u2014 Two-way FE")
  ) %>%
    reduce(full_join, by = "Variable") %>%
    mutate(across(everything(), ~ replace_na(as.character(.), "\u2014")))
}

# =============================================================================
# 7. TABLE 5: DIRECT/INDIRECT/TOTAL EFFECTS
# =============================================================================

make_table5 <- function() {
  extract_cs <- function(imp, model, crime) {
    idx    <- which(colnames(imp$sres$direct) == "corporate")
    dir    <- imp$res$direct[idx, 1]
    ind    <- imp$res$indirect[idx, 1]
    tot    <- imp$res$total[idx, 1]
    dir_se <- sd(imp$sres$direct[, idx])
    ind_se <- sd(imp$sres$indirect[, idx])
    tot_se <- sd(imp$sres$total[, idx])
    data.frame(Model = model, `Crime type` = crime,
      `Direct (SE)`   = fmt_coef(dir, dir_se, sig_stars(2*pnorm(-abs(dir/dir_se)))),
      `Indirect (SE)` = fmt_coef(ind, ind_se, sig_stars(2*pnorm(-abs(ind/ind_se)))),
      `Total (SE)`    = fmt_coef(tot, tot_se, sig_stars(2*pnorm(-abs(tot/tot_se)))),
      check.names = FALSE)
  }
  extract_panel <- function(imp, model, crime) {
    dir    <- rowMeans(imp$mimpactsdir)["corporate"]
    ind    <- rowMeans(imp$mimpactsind)["corporate"]
    tot    <- rowMeans(imp$mimpactstot)["corporate"]
    dir_se <- apply(imp$mimpactsdir, 1, sd)["corporate"]
    ind_se <- apply(imp$mimpactsind, 1, sd)["corporate"]
    tot_se <- apply(imp$mimpactstot, 1, sd)["corporate"]
    data.frame(Model = model, `Crime type` = crime,
      `Direct (SE)`   = fmt_coef(dir, dir_se, sig_stars(2*pnorm(-abs(dir/dir_se)))),
      `Indirect (SE)` = fmt_coef(ind, ind_se, sig_stars(2*pnorm(-abs(ind/ind_se)))),
      `Total (SE)`    = fmt_coef(tot, tot_se, sig_stars(2*pnorm(-abs(tot/tot_se)))),
      check.names = FALSE)
  }
  bind_rows(
    extract_cs(imp_cs_tract_pers,  "Cross-section SAR",      "Personal"),
    extract_cs(imp_cs_tract_prop,  "Cross-section SAR",      "Property"),
    extract_panel(imp_panel_pers,  "Panel SAR (one-way FE)", "Personal"),
    extract_panel(imp_panel_prop,  "Panel SAR (one-way FE)", "Property")
  )
}

# =============================================================================
# 8. TABLE 6: ROBUSTNESS CHECKS
# =============================================================================

make_table6 <- function() {
  extract <- function(mod, label, type = "sar") {
    cf    <- mod$bfixed; se <- mod$se_bfixed
    pv    <- 2 * pnorm(-abs(cf / se))
    rho_p <- 2 * pnorm(-abs(mod$rho / mod$se_rho))

    wlag_name <- "fixed_Wlag.corporate"
    rows <- bind_rows(
      data.frame(Variable = "Corporate ownership %",
                 Estimate = fmt_coef(cf["fixed_corporate"], se["fixed_corporate"],
                                     sig_stars(pv["fixed_corporate"])),
                 check.names = FALSE),
      data.frame(Variable = "W \u00d7 Corporate ownership %",
                 Estimate = if (type=="sdm" && wlag_name %in% names(cf))
                   fmt_coef(cf[wlag_name], se[wlag_name], sig_stars(pv[wlag_name]))
                   else "\u2014",
                 check.names = FALSE),
      data.frame(Variable = "Spatial lag (rho)",
                 Estimate = fmt_coef(mod$rho, mod$se_rho, sig_stars(rho_p))),
      data.frame(Variable = "N",
                 Estimate = as.character(mod$nfull)),
      data.frame(Variable = "AIC",
                 Estimate = as.character(round(mod$aic, 1)))
    )
    names(rows)[2] <- label
    rows
  }
  list(
    extract(sdm_panel_pers,    "SDM \u2014 Personal",      type="sdm"),
    extract(sdm_panel_prop,    "SDM \u2014 Property",      type="sdm"),
    extract(sar_panel_pers_k5, "SAR k=5 \u2014 Personal",  type="sar"),
    extract(sar_panel_prop_k5, "SAR k=5 \u2014 Property",  type="sar")
  ) %>%
    reduce(full_join, by = "Variable") %>%
    mutate(across(everything(), ~ replace_na(as.character(.), "\u2014")))
}

# Build tables
t1 <- make_table1()
t2 <- make_table2()
t3 <- make_table3()
t4 <- make_table4()
t5 <- make_table5()
t6 <- make_table6()

# =============================================================================
# 9. FLEXTABLES
# =============================================================================

ft1 <- flextable(t1) %>% ft_theme() %>%
  set_caption("Table 1. Descriptive Statistics") %>%
  colformat_double(j = c("Mean","Median","SD","Min","Max"), digits = 3) %>%
  colformat_num(j = "N", big.mark = ",", digits = 0) %>%
  footnote(i=1, j=1, part="header",
           value=as_paragraph("Crime rates per 1,000 residents. Ownership variables on 0-1 scale. ACS controls at tract level only; grid values assigned from parent tract via spatial crosswalk."),
           ref_symbols="a")

ft2 <- flextable(t2) %>% ft_theme() %>%
  set_caption("Table 2. Global Moran's I \u2014 Spatial Autocorrelation") %>%
  footnote(i=1, j=1, part="header",
           value=as_paragraph("Tract weights: queen contiguity (standard for administrative units with no connectivity gaps). Grid weights: k-nearest neighbours (k=5); queen contiguity excluded due to 29 disconnected subgraphs from missing parcel cells. All statistics confirm significant positive spatial clustering."),
           ref_symbols="a")

ft3 <- flextable(t3) %>% ft_theme() %>%
  set_caption("Table 3. Cross-Sectional SAR Models (2024)") %>%
  bold(i = ~ Variable == "Corporate ownership %") %>%
  footnote(i=1, j=1, part="header",
           value=as_paragraph("Dependent variable: ln(crime rate+1). Standard errors in parentheses. *** p<0.001, ** p<0.01, * p<0.05, \u2020 p<0.1. per_wht excluded (VIF=16.3, r=-0.965 with per_blk). Spatial weights: k-NN (k=4) for all models; ensures fully connected weight matrices. SAR selected via LM diagnostic tests (adjRSlag dominant). Grid models include tract fixed effects; luc.com excluded (collinear with luc.ag + luc.res). Owner-occupancy % negative and significant in grid models, consistent with guardianship mechanism. Resid. AC p: non-significant confirms spatial dependence absorbed."),
           ref_symbols="a")

ft4 <- flextable(t4) %>% ft_theme() %>%
  set_caption("Table 4. Spatial Panel SAR Models \u2014 Tract Scale (2018\u20132024)") %>%
  bold(i = ~ Variable == "Corporate ownership %") %>%
  footnote(i=1, j=1, part="header",
           value=as_paragraph("Dependent variable: ln(crime rate+1). Standard errors in parentheses. *** p<0.001, ** p<0.01, * p<0.05, \u2020 p<0.1. Panel: 149 balanced tracts, 2018-2024. per_wht excluded (VIF=16.3). Spatial weights: k-NN (k=4). Individual FE preferred; two-way FE attenuates due to low within-tract variation in ownership (median SD=0.008). Vacancy rate unavailable; guardianship mechanism cannot be directly tested at tract scale."),
           ref_symbols="a")

ft5 <- flextable(t5) %>% ft_theme() %>%
  set_caption("Table 5. Direct, Indirect, and Total Effects of Corporate Ownership") %>%
  footnote(i=1, j=1, part="header",
           value=as_paragraph("Standard errors from simulation (R=999). *** p<0.001, ** p<0.01, * p<0.05. Effects represent impact of one-unit increase in corporate ownership share; multiply by 0.10 for 10pp increase. Direct: own-unit impact including spatial feedback. Indirect: spillover to neighbouring units. Total = Direct + Indirect. Tract scale only."),
           ref_symbols="a")

ft6 <- flextable(t6) %>% ft_theme() %>%
  set_caption("Table 6. Robustness Checks \u2014 Spatial Panel Models (Individual FE, 149 Tracts, 2018\u20132024)") %>%
  bold(i = ~ Variable == "Corporate ownership %") %>%
  footnote(i=1, j=1, part="header",
           value=as_paragraph("Dependent variable: ln(crime rate+1). Standard errors in parentheses. *** p<0.001, ** p<0.01, * p<0.05, \u2020 p<0.1. SDM (k=4): spatial Durbin model; W\u00d7Corporate insignificant confirms SAR correctly specified. SAR k=5: alternative spatial weights (k=5); corporate coefficient stable and AIC improves, confirming robustness to weights choice."),
           ref_symbols="a")

# =============================================================================
# 10. WORD DOCUMENT
# =============================================================================

doc <- read_docx() %>%
  body_add_par("Corporate Ownership and Crime in St. Louis County",
               style = "heading 1") %>%
  body_add_par("Statistical Tables \u2014 Spatial Analysis", style = "Normal") %>%
  body_add_par("", style = "Normal") %>%
  body_add_flextable(ft1) %>% body_add_par("", style = "Normal") %>%
  body_add_flextable(ft2) %>% body_add_par("", style = "Normal") %>%
  body_add_flextable(ft3) %>% body_add_par("", style = "Normal") %>%
  body_add_flextable(ft4) %>% body_add_par("", style = "Normal") %>%
  body_add_flextable(ft5) %>% body_add_par("", style = "Normal") %>%
  body_add_flextable(ft6) %>% body_add_par("", style = "Normal") %>%
  body_add_par("Significance codes: *** p<0.001   ** p<0.01   * p<0.05   \u2020 p<0.1",
               style = "Normal")

outpath <- "./Analysis/Output/Tables/Spatial_Results_Tables.docx"
print(doc, target = outpath)
cat(sprintf("Word document saved: %s\n", outpath))

# =============================================================================
# 11. MAPS
# =============================================================================

tmap_mode("plot")
tmap_options(component.autoscale = FALSE)

tract_map <- core.tract %>%
  filter(year == 2024) %>%
  pivot_wider(names_from = OffenseCategory,
              values_from = c(rate, log_rate, event), names_sep = "_") %>%
  right_join(tracts_sf, by = "GEOID") %>% st_as_sf()

grid_map <- core.grid %>%
  filter(year == 2024) %>%
  pivot_wider(names_from = OffenseCategory,
              values_from = c(rate, log_rate, rate_wins, event),
              names_sep = "_") %>%
  right_join(grid_sf, by = "grid_id") %>% st_as_sf()

lisa_pal <- c("High-High"       = "#E24B4A",
              "Low-Low"         = "#378ADD",
              "High-Low"        = "#EF9F27",
              "Low-High"        = "#9FE1CB",
              "Not significant" = "#D3D1C7")

qmap <- function(sf_obj, var, title, borders = TRUE) {
  m <- tm_shape(sf_obj) +
    tm_fill(var,
            fill.scale  = tm_scale_intervals(style = "quantile", n = 5,
                                             values = "brewer.yl_or_rd"),
            fill.legend = tm_legend(title = "Quintiles")) +
    tm_title(title)
  if (borders) m <- m + tm_borders(lwd = 0.3, col = "white")
  m
}

lmap <- function(sf_obj, title, borders = TRUE) {
  m <- tm_shape(sf_obj) +
    tm_fill("lisa_quad",
            fill.scale  = tm_scale_categorical(values = lisa_pal),
            fill.legend = tm_legend(title = "Cluster type")) +
    tm_title(title)
  if (borders) m <- m + tm_borders(lwd = 0.3, col = "white")
  m
}

tmap_save(tmap_arrange(
  qmap(tract_map, "corporate", "(a) Tract: corporate ownership %"),
  qmap(grid_map,  "corporate", "(b) Grid: corporate ownership %", borders=FALSE),
  ncol=2), "./Analysis/Output/Maps/Fig1_Corporate_Ownership.png",
  width=10, height=5, dpi=300)

tmap_save(tmap_arrange(
  qmap(tract_map, "rate_Person",   "(a) Personal crime rate per 1,000"),
  qmap(tract_map, "rate_Property", "(b) Property crime rate per 1,000"),
  ncol=2), "./Analysis/Output/Maps/Fig2_Crime_Rates_Tract.png",
  width=10, height=5, dpi=300)

tmap_save(tmap_arrange(
  lmap(lisa_tract_pers, "(a) Personal crime clusters"),
  lmap(lisa_tract_prop, "(b) Property crime clusters"),
  lmap(lisa_tract_own,  "(c) Corporate ownership clusters"),
  ncol=3), "./Analysis/Output/Maps/Fig3_LISA_Tract.png",
  width=14, height=5, dpi=300)

tmap_save(tmap_arrange(
  lmap(lisa_grid_pers, "(a) Personal crime clusters",      borders=FALSE),
  lmap(lisa_grid_prop, "(b) Property crime clusters",      borders=FALSE),
  lmap(lisa_grid_own,  "(c) Corporate ownership clusters", borders=FALSE),
  ncol=3), "./Analysis/Output/Maps/Fig4_LISA_Grid.png",
  width=14, height=5, dpi=300)

tmap_save(tmap_arrange(
  qmap(grid_map, "rate_wins_Person",   "(a) Grid: personal crime rate",  borders=FALSE),
  qmap(grid_map, "rate_wins_Property", "(b) Grid: property crime rate",  borders=FALSE),
  ncol=2), "./Analysis/Output/Maps/Fig5_Grid_Crime_Rates.png",
  width=10, height=5, dpi=300)

cat("All maps saved.\n")
cat("\n03_Tables_Maps.R complete.\n")
