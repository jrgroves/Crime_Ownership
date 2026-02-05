#Uses Regex to determine the type of owner within the ownership data.
#Reads in the Parcel files and extracts IDs and Ownership Information

#Jeremy R. Groves
#May 30, 2024

#UPDATES
#June 27:  Add in 2018-2020 but used slightly different code due to different data.
#July 3:   Modified code to create a full ownership (fown) set and the non-owner occupied (noo) set
#August 1: Changed the original attempt to use regex expressions. Includes all owners without regard to tenure
#September 8: Modified to run using parallel processing
#September 17: Added combination into since OWN file long with cleaning up missing values that can be known.
#April 8, 2025: Added data up to 2024 EOY files. Also set to load off external drive for zip file extract. 
                #Also removed parallel processing.
#June 11, 2025: Added the clean strings command

rm(list=ls())

library(tidyverse)
library(foreign)
library(fedmatch)

  
#Define Values#####

y<-seq(2001,2024)

#Clean Strings Common Words Additions#####
  new_sp_char <- data.table::data.table(character = c("\\/"),
                                        replacement = c("and"))
  sp_char = data.table::rbindlist(list(sp_char_words, new_sp_char))
  
  new_corp_words <- data.table::data.table(abbr = c("l l c", "l l p", "l p", " a mocporp", "li tr", "ptnsp", "vil", "dept", "st",
                                                    "ass", "assn"),
                                           long.names = c("limited liability corporation", "limited liability partnership",
                                                          "limited partnership", "", "living trust", "partnership", "village",
                                                          "department", "saint", "association","association"))
  corp_words <- data.table::rbindlist(list(corporate_words, new_corp_words))
#Corporate Names#####
CORP <- c("\\s(ASHFIELD ACTIVE LIVING)+\\s+",
          "\\s(CASTLE POINT LIVING)+\\s+",
          "\\s(BRYANTS TOWING)+\\s+",
          "\\s(REHAB)+.*\\s+",
          "\\s(PRECISION PLUMBING SOLUTIONS)+\\s+",
          "\\s(WIRELESS)+\\s+",
          "\\s(AERO CHARTER)+\\s+",
          "\\s(AMERITECH)+\\s+",
          "\\s(AUTOMATIC ICE SYSTEMS)+\\s+",
          "\\s(BABY HAVEN)+\\s+",
          "\\s(CENTURY ALARM)+\\s+",
          "\\s(CRESTWOOD EXECUTIVE CENTER)+\\s+",
          "\\s(DOUGLASS LAND LIMITED)+\\s+",
          "\\s(DRURY INN)+\\s+",
          "\\s(DUBLEN HOMES)+\\s+",
          "\\s(EDYS GRAND ICE CREAM)+\\s+",
          "\\s(EIGHTEEN INESTMENTS)+\\s+",
          "\\s(EVERGREEN EQUITIES)+\\s+",
          "\\s(HICKORY CREST)+\\s+",
          "\\s(NORTH MERAMEC)+\\s+",
          "\\s(CAR WASH)+\\s+",
          "\\s(CARWASH)+\\s+",
          "\\s(FOUR SEVENTY SEVEN)+\\s+",
          "\\s(GALE BUILDING & REMODELING)+\\s+",
          "\\s(ATLAS TRUCK SALES)+\\s+",
          "\\s(YOUDE FAMILY)+\\s+",
          "\\s(H & M MACHINE SERVICE)+\\s+",
          "\\s(HGS DESIGN LTD)+\\s+",
          "\\s(I-44 MARINE)+\\s+",
          "\\s(JIFFY LUBE)+\\s+",
          "\\s(KINGS FOOD MARKET)+\\s+",
          "\\s(LADUE INNERBELT EXECUTIVE CENTER)+\\s+",
          "\\s(LECHNER & LECHNER LTD)+\\s+",
          "\\s(CUSTOM HOMES)+\\s+",
          "\\s(LONE STAR STEAKHOUSE)+\\s+",
          "\\s(MIDAMERICA CENTER)+\\s+",
          "\\s(NORFOLK & WESTERN RAILROAD)+\\s+",
          "\\s(OUTDOOR SYSTEMS)+\\s+",
          "\\s(P & A DRYWALL SUPPLY)+\\s+",
          "\\s(PARK CRESTWOOD LTD)+\\s+",
          "\\s(PHOENIX INTERNATIONAL)+\\s+",
          "\\s(PHOENIX INTTERNATIONAL)+\\s+",
          "\\s(PINE LAWN DENTAL)+\\s+",
          "\\s(PIZZA INN)+\\s+",
          "\\s(PLEASANT HOLLOW LTD)+\\s+",
          "\\s(R MITCHELL RENOVATIONS)+\\s+",
          "\\s(RED LOBSTER INNS OF AMERICA)+\\s+",
          "\\s(SHERWOODS FOREST NURSERY)+\\s+",
          "\\s(SPRINT PCS)+\\s+",
          "\\s(REFUSE DISPOSAL SERVICE)+\\s+",
          "\\s(SUTTON RESOURCES LTD)+\\s+",
          "\\s(TOWNHOUSES LTD)+\\s+",
          "\\s(TUCKEY & ASSOCS PHYSICAL)+\\s+",
          "\\s(UNION PACIFIC SYSTEMS)+\\s+",
          "\\s(WINTER BROTHERS MATERIALS)+\\s+",
          "\\s(WOLFF SHOE MANUFACTURING)+\\s+",
          "\\s(MARYVILLE CENTRE HOTEL)+\\s+",
          "\\s(HIGHLAND MINI STORAGE)+\\s+")

#Create Owner Data Files####
   for(i in y){
      ifelse(i < 2009,{     #Note that prior to 2010, the owner and parcel data where in different files.
        load(unz(paste0("F:/Data/Saint Louis County Assessor Data/STLCOMO_REAL_ASMTROLL_EOY_",i,".zip"),
                 filename = "pardat.RData"))
        
        ##OWNDAT Fixed Width read######
        owndat <- read_fwf(unz(paste0("F:/Data/Saint Louis County Assessor Data/STLCOMO_REAL_ASMTROLL_EOY_",i,".zip"),
                               filename = "owndat.csv"), 
                           fwf_cols(parid = 9,
                                    taxyr = 4,
                                    o_name1 = 40,
                                    o_name2 = 40,
                                    addrtryp = 1,
                                    o_adrno = 10,
                                    o_adradd = 6,
                                    o_adrdir = 2,
                                    o_adrstr = 30,
                                    o_adrsuf = 8,
                                    o_adrsuf2 = 8,
                                    o_city = 40,
                                    o_statecode = 2,
                                    o_country = 30,
                                    zip = 10,
                                    o_unitdesc = 10,
                                    o_unitno = 10,
                                    addr1 = 80,
                                    addr2 = 80,
                                    addr3 = 80,
                                    o_zip1 = 5,
                                    zip2 = 4,
                                    carrier_rt = 4,
                                    postal_inx = 10,
                                    pctown = 7,
                                    salekey = 8,
                                    conveyno = 9,
                                    type1 = 3,
                                    type2 = 3,
                                    type3 = 3,
                                    type4 = 3,
                                    notecd = 2,
                                    num = 6,
                                    link = 15,
                                    partial = 1,
                                    book = 8,
                                    page = 8,
                                    notes = 80,
                                    user = 100),
                           col_types = cols("c","n","c","c","c","n","c","c","c","c",
                                            "c","c","c","c","c","c","c","c","c","c",
                                            "c","c","c","c","c","n","n","c","c","c",
                                            "c","c","c","c","c","c","c","c","c"))
        
        owndat <- owndat %>%
          select(parid, taxyr, starts_with("o_"))
        ##Clean the Strings and Merge the data######
        co <- owndat %>%
          unite("stradr", o_adrno:o_adrsuf, sep = " ", na.rm=TRUE) %>%
          mutate(co_stradr = clean_strings(stradr),
                 co_name = clean_strings(o_name1, sp_char_words = sp_char, common_words = corp_words),
                 co_city = clean_strings(o_city),
                 co_state = clean_strings(o_statecode),
                 co_zip = clean_strings(o_zip1)) %>%
          select(parid, starts_with("co_")) %>%
          filter(co_stradr != "")
        
        po <- pardat %>%
          unite("stradr", adrno:adrsuf, sep = " ", na.rm=TRUE) %>%
          mutate(po_stradr = clean_strings(stradr),
                 po_city = clean_strings(city),
                 po_zip = clean_strings(zip1),
                 po_livunit = as.numeric(livunit)) %>%
          select(parid, starts_with("po_"), class) %>%
          filter(#class == "R",
                 !is.na(po_livunit),
                 po_livunit > 0)
        
        work <- po %>%
          left_join(., co, by = "parid", relationship = "many-to-many") %>%
          filter(!is.na(co_name)) %>%
          mutate(tenure = case_when(po_stradr == co_stradr ~ "OWNER",
                                    TRUE ~ "NONOWNER"),
                 co_name = gsub(" a mocorp","", co_name)) 
      },{
        #Post 2009 the owner and parcel data were merged into the same file
        pardat <- read.csv(unz(paste0("F:/Data/Saint Louis County Assessor Data/STLCOMO_REAL_ASMTROLL_EOY_",i,".zip"),
                             filename = "primary_parcel.csv"), sep = "|", header = TRUE, stringsAsFactors = FALSE)
      
        colnames(pardat) <- gsub("\\.", "_", colnames(pardat))
      
        work <- pardat %>%
          unite("co_stradr",OWNER_ADRNO:OWNER_SUFFIX, sep = " ", na.rm=TRUE) %>%
          unite("po_stradr",TAX_ADRNO:TAX_SUFFIX, sep = " ", na.rm=TRUE) %>%
          mutate(co_stradr = clean_strings(co_stradr),
                 po_stradr = clean_strings(po_stradr),
                 co_name = clean_strings(OWN1, sp_char_words = sp_char, common_words = corp_words),
                 co_city = clean_strings(OWNER_CITY),
                 co_state = clean_strings(OWNER_STATE),
                 co_zip = clean_strings(OWNER_ZIP),
                 po_city = clean_strings(TAX_CITY),
                 po_zip = clean_strings(TAX_ZIP),
                 po_livunit = as.numeric(LIVUNIT)) %>%
          filter(co_stradr != "",
                 #CLASS == "R",
                 !is.na(po_livunit),
                 po_livunit > 0) %>%
          mutate(tenure = case_when(po_stradr == co_stradr ~ "OWNER",
                                    TRUE ~ "NONOWNER")) %>%
          select(PARID, starts_with("co_"), starts_with("po_"), CLASS, tenure)
        
        names(work) <- tolower(names(work))
        
      })

 ##Ownership Classification Section#####     
  own_dat <- work %>%
    filter(tenure == "NONOWNER")%>%
    mutate(co_name = gsub("corpation", "corporation", co_name)) %>%  #This part of mutate correct common misspellings
    mutate(corporate = case_when(str_detect(co_name, "\\s+corporation+.*") ~ 1,
                                 str_detect(co_name, "\\s+incorporated+.*") ~ 1,
                                 str_detect(co_name, "\\s+compan+.*") ~ 1,
                                 str_detect(co_name, "\\s+rental+.*") ~ 1,
                                 str_detect(co_name, "\\s+acqui+.*") ~ 1,
                                 str_detect(co_name, "\\s+invest+.*") ~ 1,
                                 str_detect(co_name, "\\s+realt+.*") ~ 1,
                                 str_detect(co_name, "\\s+realt+.*") ~ 1,
                                 str_detect(co_name, "\\s+devel+.*") ~ 1,
                                 str_detect(co_name, "\\s+lease+.*") ~ 1,
                                 str_detect(co_name, "\\s+enter+.*") ~ 1,
                                 str_detect(co_name, "\\s+proper+.*") ~ 1,
                                 str_detect(co_name, "\\s+real est+.*") ~ 1,
                                 str_detect(co_name, "\\s+rehabilitation+.*") ~ 1,
                                 str_detect(co_name, "\\s+construction+.*") ~ 1,
                                 str_detect(co_name, "\\s+housinginc+.*") ~ 1,
                                 #str_detect(co_name, str_c(CORP, collapse = "|"))~ 1,
                                 TRUE ~ 0),
           trustee = case_when(str_detect(co_name, "\\s+pandt+") ~ 1,
                               str_detect(co_name, "\\s+trust+") ~ 1,
                               str_detect(co_name, "\\s+trustee+") ~ 1,
                               str_detect(co_name, "\\s+tande+") ~ 1,
                               str_detect(co_name, "\\s+tr+") ~ 1,
                               str_detect(co_name, "\\s+irrevocable+") ~ 1,
                               str_detect(co_name, "\\s+revocable+") ~ 1,
                               str_detect(co_name, "\\s+living+") ~ 1,
                               str_detect(co_name, "\\s+joint+") ~ 1,
                               str_detect(co_name, "\\s+group+.*") ~ 1,
                               str_detect(co_name, "\\s+venture+.*") ~ 1,
                               str_detect(co_name, "\\s+family+.*") ~ 1,
                               str_detect(co_name, "\\s+sharing+.*") ~ 1,
                               str_detect(co_name, "\\s+indent+.*") ~ 1,
                               str_detect(co_name, "\\s+estate of+.*") ~ 1,
                               str_detect(co_name, "\\s+heirs+.*") ~ 1,
                               TRUE ~ 0),
           nonprofit = case_when(str_detect(co_name, "\\s+church+.*") ~ 1,
                                 str_detect(co_name, "\\s+foundation+.*") ~ 1,
                                 str_detect(co_name, "\\s+mission+.*") ~ 1,
                                 str_detect(co_name, "\\s+sisters+.*") ~ 1,
                                 str_detect(co_name, "\\s+disciples+.*") ~ 1,
                                 str_detect(co_name, "\\s+catholic+.*") ~ 1,
                                 str_detect(co_name, "\\s+fellowship+.*") ~ 1,
                                 str_detect(co_name, "\\s+home care+.*") ~ 1,
                                 str_detect(co_name, "\\s+archbishop+.*") ~ 1,
                                 str_detect(co_name, "\\s+university+.*") ~ 1,
                                 str_detect(co_name, "\\s+christian+.*") ~ 1,
                                 str_detect(co_name, "\\s+assembly+.*") ~ 1,
                                 str_detect(co_name, "\\s+lutheran+.*") ~ 1,
                                 str_detect(co_name, "\\s+holy+.*") ~ 1,
                                 str_detect(co_name, "\\s+habitat+.*") ~ 1,
                                 str_detect(co_name, "\\s+ministries+.*") ~ 1,
                                 str_detect(co_name, "\\s+assisted+.*") ~ 1,
                                 str_detect(co_name, "\\s+health care+.*") ~ 1,
                                 str_detect(co_name, "\\s+daughters of+.*") ~ 1,
                                 str_detect(co_name, "\\s+worship+.*") ~ 1,
                                 str_detect(co_name, "\\s+society+.*") ~ 1,
                                 str_detect(co_name, "\\s+salvation+.*") ~ 1,
                                 str_detect(co_name, "\\s+episcopal+.*") ~ 1,
                                 str_detect(co_name, "\\s+evangelical+.*") ~ 1,
                                 str_detect(co_name, "\\s+academy+.*") ~ 1,
                                 str_detect(co_name, "\\s+intitute+.*") ~ 1,
                                 str_detect(co_name, "\\s+league+.*") ~ 1,
                                 str_detect(co_name, "\\s+lodge+.*") ~ 1,
                                 str_detect(co_name, "\\s+program+.*") ~ 1,
                                 str_detect(co_name, "\\s+methodist+.*") ~ 1,
                                 str_detect(co_name, "\\s+league+.*") ~ 1,
                                 TRUE ~ 0),
           reown = case_when(str_detect(co_name, "\\s+finance+.*") ~ 1,
                             str_detect(co_name, "\\s+bank+.*") ~ 1,
                             str_detect(co_name, "\\s+equity+.*") ~ 1,
                             str_detect(co_name, "\\s+mortage+.*") ~ 1,
                             str_detect(co_name, "\\s+credit+.*") ~ 1,
                             str_detect(co_name, "\\s+lender+.*") ~ 1,
                             str_detect(co_name, "\\s+savings+.*") ~ 1,
                             str_detect(co_name, "\\s+securities+.*") ~ 1,
                             str_detect(co_name, "\\s+lend+.*") ~ 1,
                             str_detect(co_name, "\\s+equicredit+.*") ~ 1,
                             str_detect(co_name, "\\s+mutual+.*") ~ 1,
                                        TRUE ~ 0),
           partnership = case_when(str_detect(co_name, "\\s+partnership+") ~ 1,
                                   str_detect(co_name, "\\s+etal+") ~ 1,
                                   str_detect(co_name, "\\s+et al+") ~ 1,
                                   TRUE ~ 0),
           private = case_when(str_detect(co_name, "\\s+handw+") ~ 1,
                               str_detect(co_name, "\\s+handh+") ~ 1,
                               str_detect(co_name, "\\s+wandw+") ~ 1,
                               str_detect(co_name, "\\s+wandh+") ~ 1,
                               TRUE ~ 0),
           hoa = case_when(str_detect(co_name, "\\s+residential+.*\\s") ~ 1,
                           str_detect(co_name, "\\s*(condominium)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(club\\s)+\\s*") ~ 1,
                           str_detect(co_name, "\\s*(ownersassociation)+.*\\s*") ~ 1, 
                           str_detect(co_name, "\\s*(residents association)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(resident association)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(woodsresidentassoc)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(estates)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(of governors)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(ofgovernors)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(neighborhood)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(estates)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(lakes at)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(manors of)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(directors)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(forestresidentass)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(reserve at)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(subdivision)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(sugartree homes)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(homes association)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(villages at)+.*\\s*") ~ 1, 
                           str_detect(co_name, "\\s*(villas at)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(plat)\\s+") ~ 1,
                           str_detect(co_name, "\\s*(casa royale)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(homes)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(ortmann overland home)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(improvement association)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(village three)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(facilities association)+.*\\s*") ~ 1,
                           str_detect(co_name, "\\s*(woodchase plaza)+.*\\s*") ~ 1,
                           TRUE ~ 0),
           muni = case_when(str_detect(co_name, "\\s+district+.*\\s") ~ 1,
                            str_detect(co_name, "\\s+authority+.*\\s") ~ 1,
                            str_detect(co_name, "\\s+city+.*\\s") ~ 1,
                            str_detect(co_name, "\\s+state+.*\\s") ~ 1,
                            str_detect(co_name, "\\s+town+.*\\s") ~ 1,
                            str_detect(co_name, "\\s+village+.*\\s") ~ 1,
                            str_detect(co_name, "\\s+sewer+.*\\s") ~ 1,
                            str_detect(co_name, "\\s+recreation+.*\\s") ~ 1,
                            str_detect(co_name, "\\s+housing and +.*\\s") ~ 1,
                            str_detect(co_name, "\\s+child and family+.*\\s") ~ 1,
                            str_detect(co_name, "\\s+school+.*\\s") ~ 1,
                            str_detect(co_name, "\\s+postal+.*\\s") ~ 1,
                            str_detect(co_name, "\\s+missouri+.*\\s") ~ 1,
                            str_detect(co_name, "\\s+saint louis+.*\\s") ~ 1,
                            str_detect(co_name, "\\s+land clearence+.*\\s") ~ 1,
                            str_detect(co_name, "\\s+transportation+.*\\s") ~ 1,
                            str_detect(co_name, "\\s+greenway+.*\\s") ~ 1,
                            str_detect(co_name, "\\s+levee+.*\\s") ~ 1,
                            TRUE ~ 0),
           key = corporate + trustee + private + partnership + nonprofit + reown + hoa + muni,
           private = case_when(key == 0 ~ 1,
                               TRUE ~ private)) %>%
    mutate(corporate = case_when(reown == 1 & corporate == 1 ~ 0,
                                 TRUE ~ corporate),
           trustee = case_when(reown == 1 & trustee == 1 ~ 0,
                               muni == 1 & trustee == 1 ~ 0,
                               TRUE ~ trustee),
           muni = case_when(muni == 1 & nonprofit == 1 ~ 0,
                            muni == 1 & corporate == 1 ~ 0,
                            reown == 1 & muni == 1 ~ 0,
                            TRUE ~ muni),
           nonprofit = case_when(reown == 1 & nonprofit == 1 ~ 0,
                               TRUE ~ nonprofit),
           hoa = case_when(reown == 1 & hoa == 1 ~ 0,
                           corporate == 1 & hoa == 1 ~ 0,
                           TRUE ~ hoa)) %>%
    distinct(., parid, .keep_all = TRUE ) 
  
  ownocc <- work %>%
    filter(tenure == "OWNER")%>%
    mutate(corporate = 0,
           trustee = 0,
           nonprofit = 0,
           reown = 0,
           hoa = 0,
           muni = 0,
           key = 0,
           partnership = 0,
           private = 1)%>%
    distinct(., parid, .keep_all = TRUE ) 
  
  own_dat <- rbind(own_dat, ownocc)
  own_dat$year <- i
  
  
  ifelse(i==2001,
         OWN <- own_dat,
         OWN <- rbind(OWN, own_dat))
  rm(own_dat)
  }
           
#Cleanup of Owner Data

fix1<-tolower(c("BALLWIN","BRIDGETON","CHESTERFIELD","EUREKA", "FENTON", "FLORISSANT","GLENCOE","GROVER",
        "HAZELWOOD","MARYLAND HEIGHTS","PACIFIC","SAINT ANN","SAINT LOUIS","VALLEY PARK"))

OWN1 <- OWN %>%
  mutate(co_state = case_when(is.na(co_state) & co_city %in% fix1 ~ "mo",
                               TRUE ~ co_state),
         co_zip = as.character(co_zip),
         co_zip = case_when(co_zip == "630" ~ "63017",
                            co_zip == "6319" ~ "63019",
                            co_zip == "2829" ~ "63144",
                            co_zip == "6314" ~ "63144",
                            co_zip == "6332" ~ "63132",
                            co_zip == "9136" ~ "91367",
                            po_zip == "8053" ~ "63031",
                             TRUE ~ co_zip),
         zip = substr(co_zip, 1, 2),
         co_state = case_when(is.na(co_state) & zip == "63" ~ "mo",
                               TRUE ~ co_state),
         po_zip = case_when(tenure == "OWNER" ~ co_zip,
                              TRUE ~ po_zip))

#Fix missing or incomplete Zip Codes for Owners####
OWN1 <- OWN1 %>%
  arrange(po_stradr, year) %>%
  mutate(po_city = case_when(tenure == "OWNER" ~ co_city,
                             TRUE ~ po_city),
         po_zip = case_when(tenure == "OWNER" ~ co_zip,
                            TRUE ~ po_zip) )%>%
  group_by(po_stradr) %>%
  fill(po_city, .direction = "downup") %>%
  fill(po_zip, .direction = "downup") %>%
  ungroup() 

  #temp1 <- OWN1 %>%
  #  filter(is.na(po_city) | is.na(po_zip)) %>%
  #  distinct(parid, .keep_all = TRUE) 
  #write.csv(temp1, file = "./build/output/temp1.csv")
  temp1 <- read.csv(file="./build/output/temp1.csv")

  OWN1 <- OWN1 %>%
    mutate(po_city = na_if(po_city, ""),
           po_stradr = na_if(po_stradr, "")) %>%
    left_join(., temp1, by=c("parid")) %>%
    mutate(po_city = coalesce(po_city, fx_city),
           po_zip = coalesce(po_zip, as.character(fx_zip)),
           po_stradr = coalesce(po_stradr, fx_stradr))
  
  #temp2 <- OWN1 %>%
  #  mutate(co_city = case_when(tenure == "OWNER" ~ po_city,
  #                             TRUE ~ co_city),
  #         co_zip = case_when(tenure == "OWNER" ~ po_zip,
  #                            TRUE ~ co_zip) )%>%
  #  filter(is.na(co_city) | is.na(co_state) | is.na(co_zip))
  #write.csv(temp2, file = "./build/output/temp2.csv")
  temp2 <- read.csv(file="./build/output/temp2.csv")
  
  OWN1 <- OWN1 %>%
    mutate(co_city = na_if(co_city, ""),
           co_stradr = na_if(co_stradr, ""),
           co_zip = na_if(co_zip, ""),) %>%
    left_join(., temp2, by=c("parid", "po_stradr")) %>%
    mutate(co_city = coalesce(co_city, fxc_city),
           co_state = coalesce(co_state, fxc_state),
           co_zip = coalesce(co_zip, as.character(fxc_zip)))
  
OWN <- OWN1 %>%
  mutate(po_zip = as.numeric(po_zip),
         co_zip = as.numeric(co_zip)) %>%
  select(-c(zip, fx_city, fx_zip, fxc_city, fxc_state, fxc_zip, fx_stradr, fxc_stradr)) %>%
  filter(po_stradr != "") %>%  #drops 11 observation 
  filter(!is.na(po_zip)) %>% #drops 9 observation 
  distinct(parid, year, .keep_all = TRUE)

save(OWN, file="./Build/Output/Own.RData")

