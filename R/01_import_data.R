library(DBI)
library(duckdb)
library(arrow)
library(config)
library(seabiRds)
library(dplyr)

# This runs the script that will establish connections to the data on the cloud
# It relies on having a valid config.yml file in your root directory, if you do not
# have a config file you can get one from me by email (allison.patterson@mail.mcgill.ca)

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
    site %in% c('Coats', 'CGM'), # Only data from Coats Island site
    species == 'TBMU', # Only data for TBMU
    time_released > as.POSIXct('2022-01-01'), # Only data from 2022
    time_recaptured < as.POSIXct('2023-12-01'),
    !is.na(gps_id) # exclude and captures that did not result in a deployment
  ) %>% 
  left_join(sex) %>% # join with sex data
  left_join(age) %>% # join with age data
  select(species, metal_band, dep_id, site, nest, 
         dep_lon,dep_lat, time_released, time_recaptured, 
         status_on, status_off, mass_on, mass_off, gps_id, 
         year_first_cap, age_first_cap, sex, sex_method, exclude) %>% # only return certain rows
  collect() # collect data from the data base

# make a list of the deployments we want to download 
dd <- deployments$dep_id[1:5] # We will limit it to the first 5 deployments for this example

# check if project contains a folder named <raw_data> create one if needed
if (dir.exists('raw_data') == F) dir.create('raw_data', recursive = T)

# Save out the deployment metadata as an RDS file
saveRDS(deployments[1:5,], 'raw_data/deployments.RDS')

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
  
}

# -----
