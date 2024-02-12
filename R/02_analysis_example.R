library(arrow)
library(dplyr)
library(seabiRds)
library(ggplot2)
theme_set(theme_light())

# ----

# read in deployment data
deployments <- readRDS('raw_data/deployments.RDS') %>% 
  select(dep_id, metal_band, site, nest, time_released, time_recaptured, 
         dep_lon, dep_lat, mass_on, mass_off, status_on, status_off, sex) 

# ----
# load the location data from the GPS dataset
gps_data <- arrow::open_dataset('raw_data/gps') %>% 
  select(dep_id, time, lon, lat) %>% 
  collect() %>% 
  arrange(dep_id, time) 

# some basic gps data processing using seabiRds

# filter out any locations are too far apart for a murre to travel
speed_threshold <- 100 # km/hr
gps_data <-do.call(rbind, 
                   lapply(unique(gps_data$dep_id), 
                          function(x) {
                            filterSpeed(data.frame(gps_data[gps_data$dep_id == x,]), threshold = speed_threshold)
                            }))

# recalculate movement metrics
gps_data <- gps_data %>% 
  inner_join(deployments) %>% 
  group_by(dep_id) %>% 
  mutate(
    coldist = getColDist(lon = lon, lat = lat, colonyLon = dep_lon[1], colonyLat = dep_lat[1]), # distance from colony in km
    dist = getDist(lon = lon, lat = lat), # distance between successive GPS fixes in km
    dt = getDT(time = time, units = 'hours'), # time between successive GPS fixes in hours
    speed = dist/dt # ground speed in km/hr
  ) %>% select(dep_id, time, lon, lat, coldist, dist, dt, speed)

# ----
# load the diving data from the tdr dataset

arrow::open_dataset('raw_data/tdr')
tdr_data <- arrow::open_dataset('raw_data/tdr') %>% 
  select(dep_id, time, temperature_c, depth_m, wet) %>% 
  collect() %>% 
  arrange(dep_id, time) 

# ----------
# load the acc data one deployment at a time, do some basic processing and join with tdr and gps data

# create an output dataframe
data <- data.frame()

for (dd in unique(tdr_data$dep_id)) {
  
  # load a single deployment 
  acc_data <- arrow::open_dataset('raw_data/acc') %>% 
    filter(dep_id %in% dd) %>% 
    select(dep_id, time, x, y, z) %>% 
    collect() 
  
  if (nrow(acc_data) > 0) {
    
    acc_data <- acc_data %>% 
      group_by(dep_id) %>% 
      arrange(dep_id, time) %>% 
      mutate(
        # calculate wing beat frequency
        wbf = seabiRds::getPeakFrequency(data = z, time = time, method = 'fft', 
                                         window = 60, threshold = 0.1, sample = 1, maxfreq = 12),
        # calculate pitch with 0 degrees standardized to angle in flight
        pitch = seabiRds::getPitch(X = x, Y = y, Z = z, time = time, window = 2, 
                                   standVar = wbf, standMin = 6, standMax = 10),
        # calculate ODBA
        odba = seabiRds::getDBA(X = x, Y = y, Z = z, time = time, window = 2)
      ) 
    
    temp <- acc_data %>% 
      filter(!is.na(wbf)) %>% 
      inner_join(tdr_data) 
    
    data <- rbind(data, temp)
  }
}

if (dir.exists('processed_data') == F) dir.create('processed_data', recursive = T)
saveRDS(data, 'processed_data/acc_data.RDS')

# -----
# combine data with gps_data, interpolate locations at frequency of data

data <- data %>% 
  left_join(gps_data, by = c("dep_id", "time")) %>% 
  inner_join(deployments[,c('dep_id','dep_lon', 'dep_lat')], by = 'dep_id') %>% 
  group_by(dep_id) %>% 
  arrange(dep_id, time) %>% 
  mutate(
    lon = imputeTS::na_interpolation(lon), # simple linear interpolation of lon
    lat = imputeTS::na_interpolation(lat), # simple linear interpolation of lat
    dist = seabiRds::getDist(lon, lat),
    dt = seabiRds::getDT(time),
    speed = dist/dt,
    coldist = seabiRds::getColDist(lon, lat, dep_lon[1], dep_lat[1])
  ) %>% 
  select(-dep_lon, -dep_lat) 

# -----

# make spatial points object
locs_sf <- sf::st_as_sf(data, coords = c('lon', 'lat'), crs = 4326)

# make tracks
tracks_sf <- locs_sf %>% 
  group_by(dep_id) %>% 
  summarize(
    tot_dist = sum(dist),
    n = n(),
    do_union=FALSE) %>% 
  filter(n > 1) %>% 
  sf::st_cast(to = 'LINESTRING')

# view tracks
mapview::mapview(tracks_sf)
