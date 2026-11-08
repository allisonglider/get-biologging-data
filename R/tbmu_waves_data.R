library(ggplot2)
theme_set(theme_light())
library(arrow)

if (!dir.exists('tbmu_waves/data')) dir.create('tbmu_waves/data', recursive = T)
dat <- readRDS('tbmu_processed/TBMU_classified_CGM_2023.RDS')
head(dat)

#mapview::mapview(dat, zcol = 'behaviour')

dat <- dat |> 
  dplyr::group_by(dep_id) |> 
  dplyr::mutate(
    session_id = seabiRds::getSessions(behaviour)
  )

head(dat)

temp <- dat |> 
  sf::st_drop_geometry() |> 
  dplyr::group_by(dep_id,session_id, behaviour) |>
  dplyr::summarise(
    start = min(time),
    end = max(time),
    duration = dplyr::n(),
  ) |> 
  dplyr::filter(duration >= 20, behaviour == 'Swimming') |> 
  dplyr::ungroup() |> 
  dplyr::mutate(
    idx = 1:dplyr::n()
  )
temp  

for (i in temp$idx) {
  dd <- temp$dep_id[i]
  ss <- temp$start[i]
  ee <- temp$end[i]
  
  acc_data <- arrow::open_dataset('raw_data/acc') |> 
    dplyr::filter(dep_id %in% dd, time > ss, time <= ee) |> 
    dplyr::filter(!is.na(z), !is.na(y),!is.na(x)) |> 
    dplyr::select(dep_id, time, x, y, z) |> 
    dplyr::collect() 
  acc_data <- acc_data[duplicated(acc_data$time) == F,]
  
  tdr_data <- arrow::open_dataset('raw_data/tdr') |>
    dplyr::filter(dep_id %in% dd, time > ss, time <= ee) |> 
    dplyr::select(dep_id, time, temperature_c, depth_m) |>
    dplyr::collect() |>
    dplyr::arrange(dep_id, time)
  
  gps_data <- arrow::open_dataset('raw_data/gps') |>
    dplyr::filter(dep_id %in% dd, time > ss, time <= ee)|> 
    dplyr::select(dep_id, time, lon, lat, altitude_m, metal_band) |>
    dplyr::collect()
  gg <- sf::st_as_sf(gps_data, coords = c('lon','lat'), crs = 4326)
  mapview::mapview(gg)
  
  dat <- acc_data |> 
    dplyr::full_join(tdr_data) |> 
    dplyr::full_join(gps_data) |>  
    dplyr::mutate(idx = i) |> 
    dplyr::select(idx,metal_band, time, x, y, z, temperature_c, depth_m, lon, lat, altitude_m) |> 
    dplyr::mutate(
      metal_band = gps_data$metal_band[1]
    ) |> 
    dplyr::group_by(idx)
  
  dat |> write_dataset('tbmu_waves/data')
}

temp <- arrow::open_dataset('tbmu_waves/data') |> 
  dplyr::filter(!is.na(lon)) |> 
  dplyr::collect()
temp <- temp |> sf::st_as_sf(coords = c('lon','lat'), crs = 4326) 
mapview::mapviewOptions(vector.palette = viridis::magma(n = max(temp$idx), begin = 0.2))
mapview::mapview(temp, zcol = 'idx', map.types = "Esri.WorldImagery",color = "black")
