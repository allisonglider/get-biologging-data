library(arrow)
library(dplyr)
library(seabiRds)
library(ggplot2)
theme_set(theme_light())

# ----

# read in deployment data
deployments <- readRDS('data/deployments.RDS') %>% 
  select(dep_id, metal_band, nest, time_released, time_recaptured, 
         dep_lon, dep_lat, mass_on, mass_off, status_on, status_off, sex) 

# ----
# load the location data from the GPS dataset
gps_data <- arrow::open_dataset('data/gps') %>% 
  select(dep_id, time, lon, lat) %>% 
  collect() %>% 
  arrange(dep_id, time) 

# some basic gps data processing using seabiRds

# Filter out any locations that require speed_threshold
speed_threshold <- 125 # km/hr
gps_data <-do.call(rbind, 
                   lapply(unique(gps_data$dep_id), 
                          function(x) {
                            filterSpeed(data.frame(gps_data[gps_data$dep_id == x,]), threshold = speed_threshold)
                            }))

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
# load the location data from the GPS dataset
arrow::open_dataset('data/tdr')
tdr_data <- arrow::open_dataset('data/tdr') %>% 
  select(dep_id, time, temperature_c, depth_m, wet) %>% 
  collect() %>% 
  arrange(dep_id, time) 

# some basic tdr data processing using seabiRds

dive_summary <- tdr_data %>% 
  filter(is.na(depth_m) == F) %>% 
  group_by(dep_id) %>% 
  mutate(
    dive_id = getSessions(depth_m > 1, ignore = T, ignoreValue = 0) # assign id to potential dives
  ) %>% 
  filter(is.na(dive_id) == F) %>% # filter out non-diving data
  group_by(dep_id, dive_id) %>% 
  summarize( # calculate dive statistics
    start = min(time),
    end = max(time),
    max_depth = max(depth_m), # maximum depth within a dive
    bottom_time = sum(depth_m >= max_depth * 0.9), # bottom time (below 90% of max dive)
    duration = n()
  ) %>% 
  filter(duration > 3, max_depth > 3) # filter out dives that do not meet minimum criteria for depth and duration

