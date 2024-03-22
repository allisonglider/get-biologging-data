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

deployments <- deployments[1:21,]
dd <- unique(deployments$dep_id)

# ----
# load the location data from the GPS dataset
gps_data <- arrow::open_dataset('raw_data/gps') |>
  filter(species == 'PEBO', dep_id %in% dd) |> 
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
  ) |>select(dep_id, time, lon, lat, coldist, dist, dt, speed)

# ----
# load the diving data from the tdr dataset

arrow::open_dataset('raw_data/tdr')
tdr_data <- arrow::open_dataset('raw_data/tdr') |>
  filter(species == 'PEBO', dep_id %in% dd) |> 
  select(dep_id, time, temperature_c, depth_m, wet) |>
  collect() |>
  arrange(dep_id, time) 

# ----------
# load the acc data one deployment at a time, do some basic processing and join with tdr and gps data

time_step <- '30 sec'


# create an output dataframe
data <- data.frame()

for (dd in unique(tdr_data$dep_id)) {
  
  g <- gps_data|>
    filter(dep_id %in% dd) |> 
    mutate(
      fly = ifelse(coldist > 0.5 & speed > 10 & !is.na(speed), 'fly', 'other'),
      fly_id = getSessions(coldist > 0.5 & speed > 10& !is.na(speed))
    ) |> 
    filter(fly == 'fly') |> 
    group_by(dep_id, fly_id) |> 
    summarize(
      start = min(time),
      end = max(time),
      dur = n()
    ) |> 
    filter(dur == max(dur)[1])
  
  gps_sf <- gps_data |>
    filter(dep_id %in% dd) |> 
    #filter(time > g$start, time < g$end) |> 
    sf::st_as_sf(coords = c('lon', 'lat'), crs = 4326)
  
  # load a single deployment 
  acc_data <- arrow::open_dataset('raw_data/acc') |>
    filter(dep_id %in% dd) |>
    select(dep_id, time, x, y, z) |>
    collect() 
  
  if (nrow(acc_data) > 0) {
    
    freq <- getFrequency(acc_data$time)
    
    pitch_cal <- acc_data |> 
      group_by(dep_id) |> 
      filter(time > g$start, time < g$end) |> 
      summarise(
        meanx = median(x),
        meany = median(y),
        meanz = median(z)
      )
    
    acc_data <- acc_data |>
      group_by(dep_id) |>
      arrange(dep_id, time) |>
      mutate(
        wbf = seabiRds::getPeakFrequency(data = z, time = time, method = 'fft',
                                         window = 30,
                                         maxfreq = 6, ###set to 6
                                         threshold = 0.2,
                                         sample = 1),
        odba = seabiRds::getDBA(X = x, Y = y, Z = z, time = time, window = 1),
        pitch = seabiRds::getPitch(X = x, #- pitch_cal$meanx, 
                                   Y = y, #- pitch_cal$meany,  
                                   Z = z, #- (1 - pitch_cal$meanz),
                                   time = time, window = 1),
      ) 
    
    p <- ggplot(acc_data[seq(1, nrow(acc_data), 100),], aes(x = time, y = pitch)) + 
      geom_line() +
      ggtitle(dd)
    print(p)
    
    temp <- acc_data |>
      filter(!is.na(wbf)) |>
      inner_join(tdr_data) |> 
      mutate(
        time = lubridate::round_date(time, time_step)
      ) |> 
      group_by(dep_id, time) |> 
      summarize(
        wbf = median(wbf),
        odba = mean(odba),
        prop_diving = sum(depth_m > 0.5)/n(),
        max_depth = max(depth_m),
        mean_pitch = mean(pitch),
        sd_pitch = sd(pitch)
      )
    
    # add to output
    data <- rbind(data, temp)
  }
}

# save processed data
if (dir.exists('processed_data') == F) dir.create('processed_data', recursive = T)
saveRDS(data, 'processed_data/acc_data.RDS')


# -----


