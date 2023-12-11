library(arrow)
library(dplyr)
library(seabiRds)
library(ggplot2)
theme_set(theme_light())


# ----

# read in deployment data
deployments <- readRDS('raw_data/deployments-PEBO.RDS') |>
  select(dep_id, metal_band, site, nest, time_released, time_recaptured, 
         dep_lon, dep_lat, mass_on, mass_off, status_on, status_off) 

# ----
# load the location data from the GPS dataset
gps_data <- arrow::open_dataset('raw_data/gps') |>
  filter(species == 'PEBO') |> 
  select(dep_id, time, lon, lat) |>
  collect() |>
  arrange(dep_id, time) 

# some basic gps data processing using seabiRds

# filter out any locations are too far apart for a murre to travel
speed_threshold <- 150 # km/hr
gps_data <-do.call(rbind, 
                   lapply(unique(gps_data$dep_id), 
                          function(x) {
                            filterSpeed(data.frame(gps_data[gps_data$dep_id == x,]), threshold = speed_threshold)
                            }))

# recalculate movement metrics
gps_data <- gps_data |>
  inner_join(deployments) |>
  group_by(dep_id) |>
  mutate(
    coldist = getColDist(lon = lon, lat = lat, colonyLon = dep_lon[1], colonyLat = dep_lat[1]), # distance from colony in km
    dist = getDist(lon = lon, lat = lat), # distance between successive GPS fixes in km
    dt = getDT(time = time, units = 'hours'), # time between successive GPS fixes in hours
    speed = dist/dt # ground speed in km/hr
  ) |>select(dep_id, time, lon, lat, coldist, dist, dt, speed)

# ----
# load the diving data from the tdr dataset

arrow::open_dataset('raw_data/tdr')
tdr_data <- arrow::open_dataset('raw_data/tdr') |>
  filter(species == 'PEBO') |> 
  select(dep_id, time, temperature_c, depth_m, wet) |>
  collect() |>
  arrange(dep_id, time) 

# ----------
# load the acc data one deployment at a time, do some basic processing and join with tdr and gps data

# create an output dataframe
data <- data.frame()

for (dd in unique(tdr_data$dep_id)) {
  
  # load a single deployment 
  acc_data <- arrow::open_dataset('raw_data/acc') |>
    filter(dep_id %in% dd) |>
    select(dep_id, time, x, y, z) |>
    collect() 
  
  if (nrow(acc_data) > 0) {
    
    freq <- getFrequency(acc_data$time)
    
    acc_data <- acc_data |>
      group_by(dep_id) |>
      arrange(dep_id, time) |>
      mutate(
        wbf = seabiRds::getPeakFrequency(data = z, time = time, method = 'fft',
                                         window = 30,
                                         maxfreq = 10, ###set to 6
                                         threshold = 0.2,
                                         sample = 1),
        # odba = seabiRds::getDBA(X = x, Y = y, Z = z, time = time, window = 60),
        # odba = zoo::rollmean(odba, k =  60 * freq, fill = NA, na.rm = T),
        Pitch = seabiRds::getPitch(X = x, Y = y, Z = z, time = time, window = 1),
      ) 
    
    temp <- acc_data |>
      filter(!is.na(wbf)) |>
      inner_join(tdr_data)|>
      left_join(gps_data, by = c("dep_id", "time")) |>
      inner_join(deployments[,c('dep_id','dep_lon', 'dep_lat')], by = 'dep_id') |>
      group_by(dep_id) |>
      arrange(dep_id, time) |>
      mutate(
        lon = imputeTS::na_interpolation(lon), # simple linear interpolation of lon
        lat = imputeTS::na_interpolation(lat), # simple linear interpolation of lat
        dist = seabiRds::getDist(lon, lat),
        dt = seabiRds::getDT(time),
        speed = dist/dt,
        coldist = seabiRds::getColDist(lon, lat, dep_lon[1], dep_lat[1])
      ) |>
      select(-dep_lon, -dep_lat) |>  
      ungroup() |> 
      slice(seq(1, n(), 5))
    
    data <- rbind(data, temp)
  }
}

if (dir.exists('processed_data') == F) dir.create('processed_data', recursive = T)
saveRDS(data, 'processed_data/acc_data_PEBO.RDS')

# -----

# make spatial points object
locs_sf <- sf::st_as_sf(data, coords = c('lon', 'lat'), crs = 4326)

# make tracks
tracks_sf <- locs_sf |>
  group_by(dep_id) |>
  summarize(
    tot_dist = sum(dist),
    n = n(),
    do_union=FALSE) |>
  filter(n > 1) |>
  sf::st_cast(to = 'LINESTRING')

# view tracks
mapview::mapview(tracks_sf)
