# This runs the script that will establish connections to the data on the cloud
# It relies on having a valid config.yml file in your root directory, if you do not
# have a config file you can get one from me by email (allison.patterson@mail.mcgill.ca)

# If this is your first time, run the three lines of code below to install specific 
# versions of packages not on CRAN. If you are running Windows and don't have Rtools installed, 
# go here to install the correct version of Rtools: https://cran.r-project.org/bin/windows/Rtools/

# install.packages("devtools")
# devtools::install_version("duckdb", version = "0.8.1", repos = "http://cran.us.r-project.org")
# devtools::install_github("allisonglider/seabiRds")

library(DBI)
library(duckdb)
library(arrow)
library(config)
library(seabiRds)
library(dplyr)
library(aws.s3)

source('R/connect_biologging_database.R')

# -----
# con is a link to the database which contains metadata about deployments and birds

# This shows us what tables exist in the database
# We will use the tables for deployments, age, and sex
dbListTables(con)

# Makes a connection to the deployment table
dep <- con %>%
  tbl("deployments") 

# Makes a connection to the gps_data table
age <- con %>%
  tbl("age") 

# Makes a connection to the sex table
sex <- con %>%
  tbl("sex") 

# -----
# Use dplyr to query deployment metadata from the biologging database

deployments <- dep %>% 
  filter(
    site %in% c('Middleton'), 
    species == 'BLKI', # Only data for TBMU
    time_released > as.POSIXct('2016-01-01'), # Only data from 2022
    !is.na(gps_id), # exclude and captures that did not result in a deployment
    
    #is.na(exclude)
  ) %>% 
  # left_join(sex) %>% # join with sex data
  # left_join(age) %>% # join with age data
  select(species, # species code
         metal_band, # metal band number - numeric
         dep_id, # unique id for deployment
         site, # colony
         nest, # nest ID - not standardized
         dep_lon, # deployment longitude
         dep_lat, # deployment latitude
         time_released, # time logger was attached in UTC
         time_recaptured, # time logger was removed in UTC
         status_on, # Breeding status at start (E - eggs, C - chicks, PB - pre-breeding, NB - non-breeder, FB - failed breeder)
         status_off, # Breeding status at end (E - eggs, C - chicks, PB - pre-breeding, NB - non-breeder, FB - failed breeder)
         mass_on, # mass at start (g)
         mass_off, # mass at end (g)
         gps_id, # GPS logger ID
         tdr_id, # TDR logger ID
         acc_id, # Accelerometer logger ID
         gls_id, # Geolocator logger ID
         # year_first_cap, # year bird was first banded
         # age_first_cap, # Age at first capture (adult or chick)
         # sex, # Sex (M/F)
         # sex_method, # Sexing method
         fed_unfed,
         exclude # Notes on any issues with the deployment
         ) %>% # only return certain rows
  collect() # collect data from the data base

# make a list of the deployments we want to download 
dd <- deployments$dep_id

# check if project contains a folder named <raw_data> create one if needed
if (dir.exists('raw_data') == F) dir.create('raw_data', recursive = T)

# Save out the deployment metadata as an RDS file
saveRDS(deployments, 'deployments.RDS')

# -----
# gps is a link to the GPS data saved as an Arrow data set on AWS S3
# Arrow lets you work efficiently with large, multi-file datasets
# For a tutorial on working with Arrow datasets, see: https://arrow.apache.org/docs/r/articles/dataset.html

# check if project contains a folder named <raw_data/gps> create one if needed
if (dir.exists('raw_data/gps') == F) dir.create('raw_data/gps', recursive = T)

# pull gps data associated with dd from the S3 dataset and save locally as an arrow dataset
gps %>% 
  filter(
    dep_id %in% dd, # Only dep_ids from our list dd
    deployed == 1 # only data collected on the bird
    ) %>% 
  collect() %>% # collect data
  group_by(site,  subsite, species,  year, metal_band, dep_id) %>%  # re-establish partitioning for arrow
  arrow::write_dataset('raw_data/gps', format = "parquet") # write as an arrow dataset

# -----
# tdr is a link to the TDR data saved as an Arrow data set on AWS S3

if (dir.exists('raw_data/tdr') == F) dir.create('raw_data/tdr', recursive = T)

# pull tdr data associated with dd from the S3 dataset and save locally as an arrow dataset

# because tdr data are recorded at a higher frequency, it can require more RAM
# to avoid filling up you local memory we'll wrap this in a for loop to download one deployment at a time

for (d in dd) {
  
  tdr %>% 
    filter(
      dep_id %in% d,
      deployed == 1
    ) %>% 
    collect() %>% 
    group_by(site,  subsite, species,  year, metal_band, dep_id) %>% 
    arrow::write_dataset('raw_data/tdr', format = "parquet")
  print(d)
}

# -----
# acc is a link to the accelerometer data saved as an Arrow data set on AWS S3

if (dir.exists('raw_data/acc') == F) dir.create('raw_data/acc', recursive = T)

# pull acc data associated with dd from the S3 dataset and save locally as an arrow dataset

for (d in dd) {
  
  acc %>% 
    filter(
      dep_id %in% d,
      deployed == 1
    ) %>% 
    collect() %>% 
    group_by(site,  subsite, species,  year, metal_band, dep_id) %>% 
    arrow::write_dataset('raw_data/acc', format = "parquet")
  print(d)
}

# -----
