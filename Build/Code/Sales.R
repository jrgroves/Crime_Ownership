#Processes the sales data file for all sales, uses BLS data to put prices into real terms

#Saves: Sales.RData - main sale data with adjusted prices

#Jeremy R. Groves
#June 27, 2024
#August 5, 2024: Added BLS data for adjusted and cleaned up files.
#April 8, 2025: Updated with 2024 EOY Sales file and read from external zip.

rm(list=ls())

library(tidyverse)
library(foreign)
library(rjson)
library(blsAPI)

#Load in Data#####
#Set the year of the EOY file to extract from
i <- 2024

#Extract and Read
temp <- read.csv(unz(paste0("F:/Data/Saint Louis County Assessor Data/STLCOMO_REAL_ASMTROLL_EOY_",i,".zip"),
                     "sales.csv"), sep = "|", header = TRUE, stringsAsFactors = FALSE)

#Get CPI data from BLS API#####

payload <- list(
  'seriesid' = c('CUSR0000SA0'),
  'startyear' = 2000,
  'endyear' = 2019,
  'registrationKey' = '8213e4e9bef14041a5f00491b6c123d6')

response <- blsAPI(payload, 2, TRUE)

cpi <- response %>%
  mutate(period = gsub("M", "", period),
         date = paste(period, "01", year, sep="-"),
         date2 = format(as.Date(date, format = "%m-%d-%Y"), "%m-%Y"),
         value = as.numeric(value)) %>%
  select(date2, value)

payload <- list(
  'seriesid' = c('CUSR0000SA0'),
  'startyear' = 2020,
  'endyear' = 2024,
  'registrationKey' = '8213e4e9bef14041a5f00491b6c123d6')

response <- blsAPI(payload, 2, TRUE)

cpi2 <- response %>%
  mutate(period = gsub("M", "", period),
         date = paste(period, "01", year, sep="-"),
         date2 = format(as.Date(date, format = "%m-%d-%Y"), "%m-%Y"),
         value = as.numeric(value)) %>%
  select(date2, value)

cpi <- rbind(cpi, cpi2)

rm(payload, response, cpi2)

#Set parameters#####

  year<-seq(2001,2024)

#Clean Sales Data#####
  
  sales.tmp <- temp %>%
    mutate(SALEVAL = case_when(SALEVAL == ".X" ~ "X",
                               SALEVAL == "i" ~ "I",
                               SALEVAL == "x" ~ "X",
                               SALEVAL == " x" ~ "X",
                               SALEVAL == "t" ~ "T",
                               SALEVAL == "TT" ~ "T",
                               SALEVAL == " T" ~ "T",
                               TRUE ~ SALEVAL),
           saledate = as.Date(SALEDT, "%d-%b-%Y"),
           date2 = format(saledate, "%m-%Y"),   #for CPI merge
           saleyr = year(saledate),
           price = as.numeric(PRICE)) %>%
    filter(SALEVAL == "4" |
             SALEVAL == "5" |
             SALEVAL == "F" |
             SALEVAL == "I" |
             SALEVAL == "P" |
             SALEVAL == "X" |
             SALEVAL == "Z" |
             SALEVAL == "T") %>%
    filter(!is.na(price)) %>%
    select(PARID, saledate, date2, price, saleyr, SALETYPE, SALEVAL) %>%
    distinct() %>%
    filter(saleyr > 2001 ) %>%
    filter(saleyr < 2025) %>%
    mutate(presale = saleyr - 1,
           postsale = saleyr + 1)

#Adjust sales prices in Sales data
  
  cpi_max <- cpi$value[which(cpi$date2==max(cpi$date2))]
  
  sales <- sales.tmp %>%
    right_join(., cpi, by="date2") %>%
    filter(price > 1000) %>%                            #Limits to nominal price greater than 1K loss of 615 obs
    mutate(adj_price = price * (cpi_max/value),
           lnadj_price = log(adj_price))

#Save main Sales Data

save(sales, file="./Build/Output/Sales.RData")
