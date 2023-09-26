# install.packages("aniMotum", 
#                  repos = c("https://cloud.r-project.org",
#                            "https://ianjonsen.r-universe.dev"),
#                  dependencies = "Suggests")

library(ggplot2)
theme_set(theme_light())

# ----

# read in deployment data
deployments <- readRDS('raw_data/deployments.RDS') %>% 
  select(dep_id, metal_band, nest, time_released, time_recaptured, 
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

# -----
library(aniMotum)

?aniMotum
?fit_ssm


dat <- gps_data |> 
  mutate(lc = "G") |> 
  select(dep_id, time, lc, lon, lat) |> 
  rename(id = dep_id, date = time)

## fit crw model to Argos LS data
fit <- fit_ssm(dat[dat$id ==dat$id[1],], vmax = 30, model = "mp", time.step = 10/60, min.dt = 1/60,
               fit.to.subset = F,
               control = ssm_control(verbose = 0)) 

fit <- route_path(fit, what = "predicted")

plot(fit, what = 'predicted')

map(fit, what = "p", normalise = TRUE, silent = TRUE)
