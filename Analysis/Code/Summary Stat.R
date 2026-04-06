# This file creates the summary statistics needed for the Data section of the paper

#By: Jeremy R. Groves
#Date: April 2, 2026

rm(list=ls())

library(flextable)
library(tidyverse)

#Default Sets for flextable package ####
  set_flextable_defaults(
    theme_fun = theme_booktabs,
    font.size = 10,
    font.family = "Times New Roman",
    digits = 2, 
    big.mark = ",")

#Data: Ownership Section####

load("./Build/Input/Own10.Rdata")
  
  
  # Limit Data to Core
  own <- OWN %>%
    filter(year > 2017) %>% 
    mutate(owner = case_when(tenure == "OWNER" ~ 1,
                             TRUE ~ 0),
           nonowner = 1 - owner,
           legal = trustee + partnership,
           other = nonprofit + reown + hoa + muni,
           class = case_when(class == "X" ~ "M",
                             class == "W" ~ "M",
                             class == "Y" ~ "M",
                             class == "Z" ~ "M",
                             TRUE ~ class)) %>%
    select(corporate, private, trustee, partnership, nonprofit, reown, 
           hoa, muni, legal, other, owner, nonowner, class, year) 

  #Summarizing the data for the table
    temp <- own %>%
      select(-year) %>%
      summarize(across(corporate:nonowner, ~mean(.x))) %>%
      mutate(class = "ALL")
    
    by.clas <- own %>%
      select(-year) %>%
     summarize(across(corporate:nonowner, ~mean(.x)), .by = class) %>%
      bind_rows(., temp) %>%
      mutate(class = case_when(class == "R" ~ "Residential",
                               class == "C" ~ "Commerical",
                               class == "A" ~ "Agricultural",
                               class == "M" ~ "Multi",
                               TRUE ~ "All")) %>%
      column_to_rownames(var = "class")
    
    names(by.clas) <- str_to_sentence(names(by.clas))

  #Creating Table One
    temp <- as.data.frame(t(by.clas))%>%
      rownames_to_column("Ownership Type") 
    
    t1 <- flextable(temp) %>%
      add_body_row(values = paste0("Observations = ", nrow(own)),
                   top = FALSE, colwidths = c(6)) %>%
      align(align = "center", part = "header") %>%
      add_header_lines(values = "Table 1: Ownership Type by Land Use Class") %>%
      colformat_double(digits = 4) %>%
      hline(i=2, part = "header") %>%
      hline(i = 8, part = "body") %>%
      hline(i = 10, part = "body") %>%
      vline(j = 5, part = "body") %>%
      bold(i = 1, part = "header") %>%
      autofit() %>%
      save_as_docx( path = "./Paper/table1.docx")

  #Creating Figure One
    temp <- own %>%
      select(year, private, corporate, legal, other) %>%
      summarize(across(private:other, mean), .by = year) %>%
      select(-private)   %>%  #Can break here to get the percentage values for each type.
      pivot_longer(cols = c("corporate", "legal", "other"),
                   names_to = "OWN_TYPE", values_to = "Share") %>%
      mutate(OWN_TYPE = str_to_title(OWN_TYPE),
             year = as.character(year))
    
    temp2 <- temp %>%
      filter(OWN_TYPE == "Corporate") %>%
      select(year, OWN_TYPE, Share) %>%
      mutate(Share = round(Share*100, 2))
    
    
    ggplot(temp, aes(fill = OWN_TYPE, x = year, y = Share)) +
      geom_bar(position = "stack", stat = "identity") +
      geom_text(aes(y = Share/100, label = Share), data = temp2, vjust = -7)+
      labs(title = "Figure One: Share of Non-Private Ownership by Year",
           x = "Year",
           fill = "Ownership Type") +
      theme_bw() +
      theme(text = element_text(family = "serif"),
            plot.title = element_text(face = "bold"))
    ggsave(file = "./Paper/Figure1.png")

#Data: Crime Section####
    
  load(file = "./Build/Output/MO_Crime_Prop.RData")

    
  #Creating Table Two: Total Crime by Year 
    temp <- CRIME %>%
      filter(year > 2017) %>%
      filter(year < 2025) %>%
      select(event, year, OffenseCategory) %>%
      aggregate(event ~ year + OffenseCategory, FUN = sum) %>%
      pivot_wider(id_cols = OffenseCategory, names_from = year, values_from = event)
    
    flextable(temp) %>%
      add_header_lines(values = "Table 2: Crime Events by Offense Category") %>%
      bold(i = 1, part = "header") %>%
      autofit() %>%
      save_as_docx( path = "./Paper/table2.docx")

#Data: Tract and Grid Sections####
    
#Creating Table 3: Summary Statistics for Crime and ownership
    load("./Build/Output/core_panels_augmented.RData")
  #Tract Level Aggregation
      temp.a <- core.tract %>%
        mutate(legal = partnership + trustee,
               other = hoa+reown+nonprofit+muni,
               type = OffenseCategory) %>%
        select(year, type, event, rate, corporate, private, legal, other, nonowner, GEOID) %>%
        filter(year > 2017) %>%
        filter(year < 2025) 
        
        temp1 <- temp.a %>%
          aggregate(rate ~ year + type, FUN = mean) %>%
          rename("Tract_Mean" = "rate")
        temp2 <- temp.a %>%
          aggregate(rate ~ year + type, FUN = max) %>%
          rename("Tract_Max" = "rate")
        temp3 <- temp.a %>%
          aggregate(rate ~ year + type, FUN = min) %>%
          rename("Tract_Min" = "rate")
        temp4 <- temp.a %>%
          aggregate(rate ~ year + type, FUN = sd) %>%
          rename("Tract_Std" = "rate")
        df.list <- list(temp1, temp2, temp3, temp4)
    
      temp.b <- df.list %>%
        reduce(left_join, by = c("year", "type")) %>%
        mutate(Year = as.character(year)) %>%
        select(type, Year, Tract_Mean, Tract_Std, 
               Tract_Min, Tract_Max)
    
      temp.a <- temp.a %>%
        distinct(GEOID, year, .keep_all = TRUE) %>%
        select(-c(type, rate, event, GEOID))   %>%
        pivot_longer(cols = -year, names_to = "type", values_to = "rate") %>%
        mutate(type = str_to_sentence(type),
               type = factor(type, levels = c("Corporate", "Private", "Legal", "Other", "Nonowner")))
      
      temp1 <- temp.a %>%
        aggregate(rate ~ year + type, FUN = mean) %>%
        rename("Tract_Mean" = "rate")
      temp2 <- temp.a %>%
        aggregate(rate ~ year + type, FUN = max) %>%
        rename("Tract_Max" = "rate")
      temp3 <- temp.a %>%
        aggregate(rate ~ year + type, FUN = min) %>%
        rename("Tract_Min" = "rate")
      temp4 <- temp.a %>%
        aggregate(rate ~ year + type, FUN = sd) %>%
        rename("Tract_Std" = "rate")
      df.list <- list(temp1, temp2, temp3, temp4)
      
      
      temp.c <- df.list %>%
        reduce(left_join, by = c("year", "type")) %>%
        mutate(Year = as.character(year)) %>%
        select(type, Year, Tract_Mean, Tract_Std, 
               Tract_Min, Tract_Max) 
      
      temp.tr <- temp.b %>%
        bind_rows(., temp.c)

  #Grid level aggregation  
      temp.a <- core.grid %>%
        mutate(legal = partnership + trustee,
               other = hoa+reown+nonprofit+muni,
               type = OffenseCategory) %>%
        select(year, type, event, rate, corporate, private, legal, other, nonowner, grid_id) %>%
        filter(year > 2017) %>%
        filter(year < 2025) 
      
      temp1 <- temp.a %>%
        aggregate(rate ~ year + type, FUN = mean) %>%
        rename("Grid_Mean" = "rate")
      temp2 <- temp.a %>%
        aggregate(rate ~ year + type, FUN = max) %>%
        rename("Grid_Max" = "rate")
      temp3 <- temp.a %>%
        aggregate(rate ~ year + type, FUN = min) %>%
        rename("Grid_Min" = "rate")
      temp4 <- temp.a %>%
        aggregate(rate ~ year + type, FUN = sd) %>%
        rename("Grid_Std" = "rate")
      df.list <- list(temp1, temp2, temp3, temp4)
      
      temp.b <- df.list %>%
        reduce(left_join, by = c("year", "type")) %>%
        mutate(Year = as.character(year)) %>%
        select(type, Year, Grid_Mean, Grid_Std, 
               Grid_Min, Grid_Max)
      
      temp.a <- temp.a %>%
        distinct(grid_id, year, .keep_all = TRUE) %>%
        select(-c(type, rate, event, grid_id))   %>%
        pivot_longer(cols = -year, names_to = "type", values_to = "rate") %>%
                mutate(type = str_to_sentence(type),
               type = factor(type, levels = c("Corporate", "Private", "Legal", "Other", "Nonowner")))
      
      
      temp1 <- temp.a %>%
        aggregate(rate ~ year + type, FUN = mean) %>%
        rename("Grid_Mean" = "rate")
      temp2 <- temp.a %>%
        aggregate(rate ~ year + type, FUN = max) %>%
        rename("Grid_Max" = "rate")
      temp3 <- temp.a %>%
        aggregate(rate ~ year + type, FUN = min) %>%
        rename("Grid_Min" = "rate")
      temp4 <- temp.a %>%
        aggregate(rate ~ year + type, FUN = sd) %>%
        rename("Grid_Std" = "rate")
      df.list <- list(temp1, temp2, temp3, temp4)
 
      temp.c <- df.list %>%
        reduce(left_join, by = c("year", "type")) %>%
        mutate(Year = as.character(year)) %>%
        select(type, Year, Grid_Mean, Grid_Std, 
               Grid_Min, Grid_Max) 

      temp.gr <- temp.b %>%
        bind_rows(., temp.c)
  
    TEMP <- temp.tr %>%
      left_join(., temp.gr, by = c("type", "Year"))
  
flextable(TEMP) %>%
  separate_header() %>%
  add_header_lines(values = "Table 3: Crime and Owner Summary by Census Tract and Grid") %>%
  align(align = "center", part = "header") %>%
  colformat_double(digits = 4) %>%
  merge_at(i = 1:7, j = "type") %>%
  merge_at(i = 8:14, j = "type") %>%
  merge_at(i = 15:21, j = "type") %>%
  merge_at(i = 22:28, j = "type") %>%
  merge_at(i = 29:35, j = "type") %>%
  merge_at(i = 36:42, j = "type") %>%
  merge_at(i = 43:49, j = "type") %>%
  set_header_labels(type = "Type",
                    Std = "Std.Dev.") %>%
  bold(i = 1, part = "header") %>%
  hline(i = 7, part = "body") %>%
  hline(i = 14, part = "body") %>%
  hline(i = 21, part = "body") %>%
  hline(i = 28, part = "body") %>%
  hline(i = 35, part = "body") %>%
  hline(i = 42, part = "body") %>%
  hline(i = 49, part = "body") %>%
  vline(j = 2, part = "body") %>%
  vline(j = 6, part = "body") %>%
  autofit() %>%
  save_as_docx( path = "./Paper/table3.docx")
