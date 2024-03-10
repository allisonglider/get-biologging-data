library(dplyr)
library(seabiRds)
library(ggplot2)
#devtools::install_github("allisonglider/seabiRds")

dep <- read.csv('raw_data/TBMU_deployments_MAM.csv')
gps <- read.csv('raw_data/TBMU_gps_data_MAM.csv')
tdr <- read.csv('raw_data/TBMU_tdr_data_MAM.csv')

cc <- unique(dep[,c('dep_lon', 'dep_lat')])
col_loc <- sf::st_as_sf(cc, coords = c('dep_lon', 'dep_lat'), crs = 4326)
mapview::mapview(col_loc)

# -----

dep$time_released <- as.POSIXct(dep$time_released, format = '%Y-%m-%d %T', tz = 'UTC')
dep$time_recaptured <- as.POSIXct(dep$time_recaptured, format = '%Y-%m-%d %T', tz = 'UTC')
gps$time <- as.POSIXct(gps$time, format = '%Y-%m-%d %T', tz = 'UTC')
tdr$time <- as.POSIXct(tdr$time, format = '%Y-%m-%d %T', tz = 'UTC')

gps <- gps |> arrange(dep_id, time)
# filter out any locations are too far apart for a murre to travel
speed_threshold <- 125 # km/hr
gps <-do.call(rbind, 
                   lapply(unique(gps$dep_id), 
                          function(x) {
                            filterSpeed(data.frame(gps[gps$dep_id == x,]), threshold = speed_threshold)
                          }))
gps_sf <- sf::st_as_sf(gps, coords = c('lon', 'lat'), crs = 4326)
mapview::mapview(gps_sf)

# -----

data <- gps |> 
  select(dep_id, metal_band, time, lon, lat) |> 
  full_join(tdr) |> 
  arrange(dep_id, time) |>
  mutate(
    lon = imputeTS::na_interpolation(lon),
    lat = imputeTS::na_interpolation(lat),
    depth_m = imputeTS::na_interpolation(depth_m),
    coldist = seabiRds::getColDist(lon, lat, cc$dep_lon[1], cc$dep_lat[1]),
    behaviour = ifelse(depth_m > 2, 'diving',
                       ifelse(coldist > 1, 'offcolony','oncolony'))
  )

plot(coldist ~ time, data[data$dep_id == unique(data$dep_id)[3],], type = 'l')

dep <- dep |> 
  select(dep_id, time_released, time_recaptured, mass_on, mass_off) |> 
  mutate(
    dmass = mass_off - mass_on,
    dep_time = as.numeric(difftime(time_recaptured, time_released, units = 'hours'))
  )

sum_data <- data |> 
  group_by(dep_id, metal_band) |> 
  summarise(
    oncolony = sum(behaviour == 'oncolony')/60,
    offcolony = sum(behaviour %in% c('offcolony'))/60,
    diving = sum(behaviour == 'diving')/60,
    track_end = max(time, na.rm = T)
  ) |> 
  inner_join(dep) |> 
  mutate(
    track_gap = as.numeric(difftime(time_recaptured, track_end, units = 'hours'))
  ) 

plot(dmass ~ oncolony, sum_data)
plot(dmass ~ offcolony, sum_data)
plot(oncolony ~ offcolony, sum_data)
plot(dmass ~ diving, sum_data)

m1 <- lm(dmass ~ I(oncolony + offcolony + diving) + diving -1, sum_data)
summary(m1)
plot(m1)

m2 <- lm(dmass ~ oncolony + offcolony + diving - 1, sum_data)
summary(m2)

m3 <- lm(dmass ~ oncolony + I(offcolony + diving) + diving - 1, sum_data)
summary(m3)

