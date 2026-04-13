library(RColorBrewer)
library(cowplot)

#Figure 2######
quants.t <- quantile(tract_map$corporate, probs = seq(0, 1, length.out = 5), na.rm = TRUE)
temp <- tract_map %>%
  mutate(Quartile = case_when(corporate <= quants.t[1] ~ "Q1",
                              corporate > quants.t[1] & corporate <= quants.t[2] ~ "Q2",
                              corporate > quants.t[2] & corporate <= quants.t[3] ~ "Q3",
                              corporate > quants.t[4] ~ "Q4",
                              TRUE ~ NA),
         Type = "Census Tract") %>%
  select(GEOID, Quartile, Type)
quants.g <- quantile(grid_map$corporate, probs = seq(0, 1, length.out = 5), na.rm = TRUE)
temp2 <- grid_map %>%
  mutate(Quartile = case_when(corporate <= quants.g[1] ~ "Q1",
                              corporate > quants.g[1] & corporate <= quants.g[2] ~ "Q2",
                              corporate > quants.g[2] & corporate <= quants.g[3] ~ "Q3",
                              corporate > quants.g[4] ~ "Q4",
                              TRUE ~ NA),
         Type = "Grid Cell",
         GEOID = as.character(grid_id)) %>%
  select(GEOID, Quartile, Type) %>%
  bind_rows(., temp)
quart.color <- c("Q1" = "#1A85FF", "Q2" = "#5D3A9B", "Q3" = "#FFC20A", "Q4" = "#DC3220")

ggplot(temp2) +
  geom_sf(aes(fill = Quartile), color = "gray80") +
  labs(fill = "Quartiles",
       title = "Figure 2: Corporate Ownership Share by Quartile (2024)") +
  scale_fill_manual(values = quart.color, drop = FALSE, na.value = NA) +
  theme_void() +
  theme(legend.position = "bottom") +
  facet_grid(~Type)

quants.g <- quantile(grid_map$corporate, probs = seq(0, 1, length.out = 5), na.rm = TRUE)

f2b<- ggplot(grid_map) +
  geom_sf(aes(fill = corporate)) +
  labs(fill = "Quartiles") +
  scale_fill_stepsn(breaks = quants.g, labels = round(quants.g, 2), colors = brewer.pal(5,"YlOrRd" ),
                    na.value = NA)+
  theme_void() +
  theme(legend.position = "bottom") 

plot_grid(f2a, f2b, labels = c("Census Tracts", "Grid Cells"), vjust = 4.5, label_fontface = "plain",
          label_size = 10) +
  draw_plot_label(label="Figure 2 Corporate Ownership Share: 2024", 
                  fontface = "bold", vjust = 1.0, size =  12)
ggsave(file = "./Paper/Figure2.png")

#Figure 3######

quants.t <- quantile(tract_map$log_rate_Property, probs = seq(0, 1, length.out = 5), na.rm = TRUE)

f3a<- ggplot(tract_map) +
  geom_sf(aes(fill = log_rate_Property)) +
  labs(fill = "Quartiles") +
  scale_fill_stepsn(breaks = quants.t, labels = round(quants.t, 1), colors = brewer.pal(5,"YlOrRd" ),
                    na.value = NA)+
  theme_void() +
  theme(legend.position = "bottom") 

quants.g <- quantile(grid_map$log_rate_Property, probs = seq(0, 1, length.out = 5), na.rm = TRUE)

f3b<- ggplot(grid_map) +
  geom_sf(aes(fill = log_rate_Property), color = NA) +
  labs(fill = "Quartiles") +
  scale_fill_stepsn(breaks = quants.g, labels = round(quants.g, 1), colors = brewer.pal(5,"YlOrRd" ),
                    na.value = NA)+
  theme_void() +
  theme(legend.position = "bottom") 

quants.t <- quantile(tract_map$log_rate_Person, probs = seq(0, 1, length.out = 5), na.rm = TRUE)
f3c<- ggplot(tract_map) +
  geom_sf(aes(fill = log_rate_Person)) +
  labs(fill = "Quartiles") +
  scale_fill_stepsn(breaks = quants.t, labels = round(quants.t, 1), colors = brewer.pal(5,"YlOrRd" ),
                    na.value = NA)+
  theme_void() +
  theme(legend.position = "bottom") 

quants.g <- quantile(grid_map$log_rate_Person, probs = seq(0, 1, length.out = 5), na.rm = TRUE)

f3d<- ggplot(grid_map) +
  geom_sf(aes(fill = log_rate_Person), color = NA) +
  labs(fill = "Quartiles") +
  scale_fill_stepsn(breaks = quants.g, labels = round(quants.g, 1), colors = brewer.pal(5,"YlOrRd" ),
                    na.value = NA)+
  theme_void() +
  theme(legend.position = "bottom") 

plot_grid(f3a, f3b, f3c, f3d,
          nrow = 2,
          labels = c("Census Tracts", "Grid Cells", "Census Tracts", "Grid Cells"), 
          vjust = 4.5, label_fontface = "plain",
          label_size = 10) +
  draw_figure_label(label="Figure 3 Crime Rates (logged) 2024", 
                  position = "top.left", fontface = "bold")
ggsave(file = "./Paper/Figure3.png")





#Figure 4#####

temp2 <- lisa_tract_own %>%
  select(GEOID, lisa_quad, lisa_I, lisa_p) %>%
  mutate(type = "Corporate Ownership")

temp3 <- lisa_tract_prop %>%
  select(GEOID, lisa_quad, lisa_I, lisa_p) %>%
  mutate(type = "Property")

temp <- lisa_tract_pers %>%
  select(GEOID, lisa_quad, lisa_I, lisa_p) %>%
  mutate(type = "Person") %>%
  bind_rows(., temp2, temp3)

lisa_pal <- c("High-High"       = "#E24B4A",
              "Low-Low"         = "#378ADD",
              "High-Low"        = "#EF9F27",
              "Low-High"        = "#9FE1CB",
              "Not significant" = "#D3D1C7")

ggplot(temp) +
  geom_sf(aes(fill = lisa_quad)) +
  scale_fill_manual(values = lisa_pal, drop = FALSE) +
  labs(fill = "Cluster Type",
       title = "Figure 4 LISA Clustering For Tracts in 2024") +
  theme_void() +
  theme(legend.position = "bottom")+
  facet_wrap(~ type)
ggsave(file = "./Paper/Figure4.png")

#Figure 5#####

temp2 <- lisa_grid_own %>%
  select(GEOID, lisa_quad, lisa_I, lisa_p) %>%
  mutate(type = "Corporate Ownership")

temp3 <- lisa_grid_prop %>%
  select(GEOID, lisa_quad, lisa_I, lisa_p) %>%
  mutate(type = "Property")

temp <- lisa_grid_pers %>%
  select(GEOID, lisa_quad, lisa_I, lisa_p) %>%
  mutate(type = "Person") %>%
  bind_rows(., temp2, temp3) 


lisa_pal <- c("High-High"       = "#E24B4A",
              "Low-Low"         = "#378ADD",
              "High-Low"        = "#EF9F27",
              "Low-High"        = "#9FE1CB",
              "Not significant" = "#D3D1C7")

ggplot(temp) +
  geom_sf(aes(fill = lisa_quad), color = NA) +
  scale_fill_manual(values = lisa_pal, drop = FALSE) +
  labs(fill = "Cluster Type",
       title = "Figure 5 LISA Clustering For Grid Cells in 2024") +
  theme_void() +
  theme(legend.position = "bottom")+
  facet_wrap(~ type)
ggsave(file = "./Paper/Figure5.png")
