library(arrow)
library(dplyr)
library(seabiRds)
library(ggplot2)
theme_set(theme_light())

# ----

# read in deployment data
deployments <- readRDS('deployments.RDS') |> 
  filter(!is.na(acc_id)) |> 
  select(dep_id, metal_band, site, nest, time_released, time_recaptured, 
         dep_lon, dep_lat, mass_on, mass_off, status_on, status_off, sex, gps_id) 

all_dd <- sort(unique(deployments$gps_id))
old_dd <- sort(unique(deployments$gps_id[deployments$time_released < as.POSIXct('2020-01-01')]))
flip_dd <- c("AA13","AA14","AA15","AA16","AA17","AA18","AA19",
             "AA20","AA21","AA22","AA23","AA24","AA25","AA26","AA27","AA28","AA29",
             "AA30","AA31","AA32","AA33","AA34","AA35","AA36","AA37",
             "AA40","AA41","AA42","AA43","AA44","AA45","AA46","AA47","AA48","AA49",
             "AA50","AA51","AA52","AA53","AA54","AA55","AA56","AA57","AA58","AA59",
             "AA61","AA62",
             "L19","L22","L23","L24","L27","L31","L37","L38","L39","L40")
# ----
# load the location data from the GPS dataset
gps_data <- arrow::open_dataset('raw_data/gps') |> 
  select(dep_id, time, lon, lat) |> 
  collect() |> 
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
gps_data <- gps_data |> 
  inner_join(deployments) |> 
  group_by(dep_id) |> 
  mutate(
    coldist = getColDist(lon = lon, lat = lat, colonyLon = dep_lon[1], colonyLat = dep_lat[1]), # distance from colony in km
    dist = getDist(lon = lon, lat = lat), # distance between successive GPS fixes in km
    dt = getDT(time = time, units = 'hours'), # time between successive GPS fixes in hours
    speed = dist/dt # ground speed in km/hr
  ) |> select(dep_id, time, lon, lat, coldist, dist, dt, speed)

# ----
# load the diving data from the tdr dataset

# arrow::open_dataset('raw_data/tdr')


# ----------
# load the acc data one deployment at a time, do some basic processing and join with tdr and gps data

# create an output directory
if (dir.exists('tbmu_data') == F) dir.create('tbmu_data', recursive = T)

all_deps <- unique(deployments$dep_id)
all_deps <- all_deps[!(all_deps %in% sub('.RDS', '',list.files('tbmu_data')))]

for (dd in all_deps) {
  
  of <- paste0('tbmu_data/',dd,'.RDS')
  if (file.exists(of) == FALSE) {
    
    # load a single deployment 
    acc_data <- arrow::open_dataset('raw_data/acc') |> 
      filter(dep_id %in% dd) |> 
      filter(!is.na(z), !is.na(y),!is.na(x)) |> 
      select(dep_id, time, x, y, z) |> 
      collect() 
    acc_data <- acc_data[duplicated(acc_data$time) == F,]
    
    tdr_data <- arrow::open_dataset('raw_data/tdr') |>
      filter(dep_id %in% dd) |> 
      select(dep_id, time, temperature_c, depth_m, wet) |>
      collect() |>
      arrange(dep_id, time)
    
    
    if (nrow(acc_data) > 0 & nrow(tdr_data) > 0) {
      
      
      if (deployments$gps_id[deployments$dep_id == dd] %in% flip_dd) acc_data <- checkAxes(acc_data, ask = F, force = T)
      f <- getFrequency(acc_data$time)
      
      acc_data <- acc_data |> 
        group_by(dep_id) |> 
        arrange(dep_id, time) |> 
        mutate(
          # calculate wing beat frequency
          wbf = seabiRds::getPeakFrequency(data = z, time = time, method = 'fft', frequency = f,
                                           window = 30, threshold = 0.1, sample = 1, maxfreq = 12),
          # calculate pitch with 0 degrees standardized to angle in flight
          pitch = seabiRds::getPitch(X = x, Y = y, Z = z, time = time, window = 2, frequency = f,
                                     standVar = wbf, #if (max(wbf, na.rm = T) > 7) wbf else NULL, 
                                     standMin = 6,#if (max(wbf, na.rm = T) > 7) 6 else NULL, 
                                     standMax = 10),#if (max(wbf, na.rm = T) > 7) 10 else NULL),
          # # calculate ODBA
          odba = seabiRds::getDBA(X = x, Y = y, Z = z, time = time, window = 2, frequency = f),
          staticx = zoo::rollmean(x, f * 2, na.pad = T),
          staticy = zoo::rollmean(y, f * 2, na.pad = T),
          staticz = zoo::rollmean(z, f * 2, na.pad = T),
          vesba = sqrt(staticx^2 + staticy^2 + staticz^2),
        ) |> 
        select(-staticx, -staticy, -staticz)
      
      temp <- acc_data |> 
        filter(!is.na(wbf)) |> 
        inner_join(tdr_data) 
      
      p <- temp |> 
        select(dep_id, time, wbf, pitch, odba, vesba) |> 
        tidyr::pivot_longer(cols = c('wbf', 'pitch', 'odba', 'vesba')) |> 
        ggplot(aes(x = time, y = value)) +
        geom_line() +
        facet_grid(rows = vars(name), scales = 'free')+
        ggtitle(dd)
      print(p)
      
      print(paste(of, 'finished'))
      saveRDS(temp, paste0('tbmu_data/',dd,'.RDS'))
    } else {print(paste('no data', dd))}
  } else print(paste(of, 'already exists'))
}



# # -----
# # combine data with gps_data, interpolate locations at frequency of data
# 
# data <- data |> 
#   left_join(gps_data, by = c("dep_id", "time")) |> 
#   inner_join(deployments[,c('dep_id','dep_lon', 'dep_lat')], by = 'dep_id') |> 
#   group_by(dep_id) |> 
#   arrange(dep_id, time) |> 
#   mutate(
#     lon = imputeTS::na_interpolation(lon), # simple linear interpolation of lon
#     lat = imputeTS::na_interpolation(lat), # simple linear interpolation of lat
#     dist = seabiRds::getDist(lon, lat),
#     dt = seabiRds::getDT(time),
#     speed = dist/dt,
#     coldist = seabiRds::getColDist(lon, lat, dep_lon[1], dep_lat[1])
#   ) |> 
#   select(-dep_lon, -dep_lat) 
# 
# # -----
# 
# # make spatial points object
# locs_sf <- sf::st_as_sf(data, coords = c('lon', 'lat'), crs = 4326)
# 
# # make tracks
# tracks_sf <- locs_sf |> 
#   group_by(dep_id) |> 
#   summarize(
#     tot_dist = sum(dist),
#     n = n(),
#     do_union=FALSE) |> 
#   filter(n > 1) |> 
#   sf::st_cast(to = 'LINESTRING')
# 
# # view tracks
# mapview::mapview(tracks_sf)
