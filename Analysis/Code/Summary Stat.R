

desc_var <- function(x) {
  data.frame(N      = sum(!is.na(x)),
             Mean   = round(mean(x, na.rm=TRUE), 3),
             Median = round(median(x, na.rm=TRUE), 3),
             SD     = round(sd(x, na.rm=TRUE), 3),
             Min    = round(min(x, na.rm=TRUE), 3),
             Max    = round(max(x, na.rm=TRUE), 3))
}


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
load("./Build/Input/Own10.RData")

own <- OWN %>%
  filter(year > 2014) %>% 
  mutate(owner = case_when(tenure == "OWNER" ~ 1,
                           TRUE ~ 0),
         nonowner = 1 - owner) %>%
  select(corporate, private, trustee, nonprofit, reown, partnership, private, hoa, muni, owner, nonowner)

vars_tract <- list(
  "Corporate ownership %"           = own$corporate,
  "Trustee %"                       = own$trustee,
  "Nonprofit %"                     = own$nonprofit,
  "REO %"                           = own$reown,
  "Partnership %"                   = own$partnership,
  "HOA %"                           = own$hoa,
  "Municipal %"                     = own$muni,
  "Owner Occupied"                  = own$owner,
  "Nonowner Occupied"               = own$nonowner
)

tract_df <- map_dfr(names(vars_tract), ~ {
  desc_var(vars_tract[[.x]]) %>% mutate(Variable = .x)
})  %>% 
  select(Variable, N, Mean, Median, SD, Min, Max)
