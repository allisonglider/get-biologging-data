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
library(dbplyr)
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
    site %in% c('CGM'), 
    species == 'TBMU', # Only data for TBMU
    time_released > as.POSIXct('2022-01-01'), # Only data from 2022
    time_recaptured < as.POSIXct('2023-12-01'),
    status_on == 'E',
    !is.na(acc_id) # exclude and captures that did not result in a deployment
  ) %>% 
  left_join(sex) %>% # join with sex data
  left_join(age) %>% # join with age data
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
         # gls_id, # Geolocator logger ID
         # year_first_cap, # year bird was first banded
         # age_first_cap, # Age at first capture (adult or chick)
         sex, # Sex (M/F)
         # sex_method, # Sexing method
         exclude # Notes on any issues with the deployment
         ) %>% # only return certain rows
  collect() # collect data from the data base

deployments <- deployments |> 
  filter(as.numeric(difftime(time_recaptured, time_released, units = 'hours')) <= 26,)

# make a list of the deployments we want to download 
dd <- deployments$dep_id # We will limit it to the first 5 deployments for this example

# check if project contains a folder named <raw_data> create one if needed
if (dir.exists('raw_data') == F) dir.create('raw_data', recursive = T)

# Save out the deployment metadata as an RDS file
write.csv(deployments, 'raw_data/TBMU_deployments_MAM.csv', row.names = F)

# -----
# gps is a link to the GPS data saved as an Arrow data set on AWS S3
# Arrow lets you work efficiently with large, multi-file datasets
# For a tutorial on working with Arrow datasets, see: https://arrow.apache.org/docs/r/articles/dataset.html

# check if project contains a folder named <raw_data/gps> create one if needed
if (dir.exists('raw_data/gps') == F) dir.create('raw_data/gps', recursive = T)

# pull gps data associated with dd from the S3 dataset and save locally as an arrow dataset
gps_data <- gps %>% 
  filter(
    dep_id %in% dd, # Only dep_ids from our list dd
    deployed == 1 # only data collected on the bird
    ) %>% 
  select(dep_id, metal_band, time, lon, lat, altitude_m, satellites, hdop) |> 
  collect() 
write.csv(gps_data, 'raw_data/TBMU_gps_data_MAM.csv', row.names = F)


tdr_data <-  tdr %>% 
  filter(
    dep_id %in% dd,
    deployed == 1
  ) %>% 
  select(dep_id, metal_band, time, temperature_c, depth_m, wet) |> 
  collect() 
write.csv(tdr_data, 'raw_data/TBMU_tdr_data_MAM.csv', row.names = F)
