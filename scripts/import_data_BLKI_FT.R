library(DBI)
library(RPostgreSQL)
library(RPostgres)
library(arrow)
library(config)
library(seabiRds)
library(dplyr)

set.seed(543)

# This runs the script tht will establish connections to the data on the cloud
source('scripts/connect_biologging_database.R')

# -----
# con is a link to the database which contains metadata about deployments and birds

# This shows us what tables exist in the database
# We will use the tables for deployments, age, and sex
dbListTables(con)

# Makes a connection to the deployment table
dep <- con |>
  tbl("deployments") 

# Makes a connection to the gps_data table
age <- con |>
  tbl("age") 

# Makes a connection to the sex table
sex <- con |>
  tbl("sex") 

deployments <- dep |> 
  filter(
    site == 'Middleton', 
    species == 'BLKI',
    #time_released > as.POSIXct('2016-01-01'), 
    #time_recaptured < as.POSIXct('2023-01-01'),
    !is.na(gps_id)
  ) |> 
  left_join(sex) |> 
  left_join(age) |> 
  select(species, metal_band, dep_id, site, nest, 
         dep_lon,dep_lat, time_released, time_recaptured, 
         status_on, status_off, mass_on, mass_off, gps_id, fed_unfed,
         year_first_cap, age_first_cap, sex, sex_method, exclude) |> 
  collect()

deployments <- deployments |> 
  mutate(
    year = as.numeric(strftime(time_released, '%Y'))
  ) |> filter(
    year %in% c(2016, 2018, 2020), 
    !is.na(time_recaptured),
    status_on == 'C'
    ) |> 
  group_by(year) |> 
  slice_sample(n = 10, replace = T) |> 
  unique()

saveRDS(deployments, 'data/deployments.RDS')

# ------

deployments <- readRDS('data/deployments.RDS')

# make a list of the deployments we want to download 
dd <- deployments$dep_id

# -----
# gps is a link to the GPS data saved as an Arrow data set on AWS S3
# Arrow lets you work efficiently with large, multi-file datasets
# For a tutorial on working with Arrow datasets, see: https://arrow.apache.org/docs/r/articles/dataset.html

# check if project contains a folder named <data/gps> create one if needed
if (dir.exists('data/gps') == F) dir.create('data/gps', recursive = T)

# pull gps data associated with dd from the S3 dataset and save locally as an arrow dataset

for (d in dd) {
  gps |> 
    filter(
      dep_id %in% d,
      deployed == 1
    ) |> 
    collect() |> 
    group_by(site,  subsite, species,  year, metal_band, dep_id) |> 
    arrow::write_dataset('data/gps', format = "parquet")
}

# -----
# acc is a link to the accelerometer data saved as an Arrow data set on AWS S3

if (dir.exists('data/acc') == F) dir.create('data/acc', recursive = T)

# pull tdr data associated with dd from the S3 dataset and save locally as an arrow dataset
for (d in dd) {
acc |> 
  filter(
    dep_id %in% d,
    deployed == 1
  ) |> 
  collect() |> 
  group_by(site,  subsite, species,  year, metal_band, dep_id) |> 
  arrow::write_dataset('data/acc', format = "parquet")
}


