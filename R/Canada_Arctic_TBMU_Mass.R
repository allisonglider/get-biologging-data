require(DBI)
require(duckdb)
require(dplyr)
require(arrow)
require(config)
require(dbplyr)

db_loc <- 'raw_data/biologging-db.duckdb'

con <- duckdb::dbConnect(duckdb::duckdb(), dbdir = db_loc, read_only = T)

dep <- con |> 
  tbl("deployments") 
sex <- con |> 
  tbl("sex") 

deployments <- dep %>% 
  filter(
    species == 'TBMU', # Only data for TBMU
    !is.na(mass_on)
  ) |> 
  left_join(sex) |>  # join with sex data
  select(
    species, # species code
         metal_band, # metal band number - numeric
         site, # colony
         time_released, # time logger was attached in UTC
         status_on, # Breeding status at start (E - eggs, C - chicks, PB - pre-breeding, NB - non-breeder, FB - failed breeder)
         mass_on, # mass at start (g)
         sex, # Sex (M/F)
         ) |> 
  collect()

table(deployments$site)

write.csv(deployments, 'raw_data/Canada_Arctic_TBMU_Mass.csv', row.names = F)


