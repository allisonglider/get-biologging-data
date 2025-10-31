
library(momentuHMM)
library(raster)
library(ggplot2)
library(dplyr)
theme_set(theme_light())

dep_data <- readRDS('deployments.RDS')

dee <- read.csv('tbmu_processed/tbmu_daily_activity.csv') 
out <- data.frame()

for (dd in unique(dee$dep_id)) {
  
  print(paste0('Starting:', dd))
  
  tdr_data <- arrow::open_dataset('raw_data/tdr') |>
    filter(dep_id %in% dd) |> 
    select(dep_id, time, temperature_c, depth_m, wet) |>
    collect() |>
    arrange(dep_id, time) |> 
    dplyr::mutate(
      depth_m = ifelse(depth_m > 250, NA, depth_m),
      depth_m = imputeTS::na_interpolation(depth_m),
      vspeed = depth_m - dplyr::lag(depth_m)
    )
  
  while (max(abs(tdr_data$vspeed), na.rm = T) > 4) {
    print(paste('Filtering', sum(abs(tdr_data$vspeed)>4, na.rm = T), 'vertical speed'))
    tdr_data <- tdr_data |> 
      dplyr::mutate(
        vspeed = depth_m - dplyr::lag(depth_m),
        depth_m = ifelse(abs(vspeed) > 3, NA, depth_m),
        depth_m = imputeTS::na_interpolation(depth_m)
      )
  }
  
  tdr_data <- tdr_data |> 
    dplyr::mutate(
      q10_depth = zoo::rollapply(depth_m, 15 * 60, FUN = quantile, 0.1, partial = TRUE),
      depth_cor = depth_m - q10_depth,
      depth_cor = ifelse(depth_cor < 0, 0, depth_cor)
    )
  
  # ggplot(tdr_data) +
  #   geom_line(aes(x = time, y = depth_m)) +
  #   geom_line(aes(x = time, y = q10_depth), col = 'blue') +
  #   geom_line(aes(x = time, y = depth_cor), col = 'red') +
  #   scale_y_reverse()
  # 
  # hist(tdr_data$depth_m - tdr_data$depth_cor, 50)
  # hist(tdr_data$depth_cor, 50)
  
  if (max(tdr_data$depth_m, na.rm = T) > 1) { 
  
  dive_sum <- tdr_data |> 
    dplyr::mutate(
      diving = ifelse(depth_cor > 1, 1, 0),
      dive_id = seabiRds::getSessions(diving == 1, ignore = TRUE, ignoreValue = 0),
      dive_id = imputeTS::na_locf(dive_id, na_remaining = "keep")
    ) |> 
    dplyr::group_by(dep_id, dive_id, diving) |> 
    dplyr::mutate(
      duration_s = dplyr::n(),
    ) |> 
    dplyr::ungroup() |> 
    dplyr::mutate(
      diving = ifelse(duration_s <=3, 0, diving),
      dive_id = seabiRds::getSessions(diving == 1, ignore = TRUE, ignoreValue = 0),
      dive_id = imputeTS::na_locf(dive_id, na_remaining = "keep")
    ) |> 
    dplyr::group_by(dep_id, dive_id, diving) |> 
    dplyr::summarise(
      start = min(time),
      end = max(time),
      duration_s = dplyr::n(),
      max_depth_m = max(depth_cor),
      .groups = 'drop'
    ) |> 
    dplyr::arrange(start) |> 
    dplyr::ungroup() |> 
    dplyr::mutate(
      post_pause_s = dplyr::lead(duration_s)
    ) |> 
    dplyr::filter(diving == 1)
  
  # hist(dive_sum$max_depth_m, 30)
  # hist(dive_sum$duration_s, 30)
  # ggplot(dive_sum) +
  #   geom_point(aes(x = max_depth_m, y = duration_s)) +
  #   scale_y_log10() +
  #   scale_x_log10()
  # 
  # dive_sum[dive_sum$max_depth_m < 3,]
  # dive_sum[dive_sum$duration_s < 5,]
  out <- rbind(out, dive_sum)
  }
  
}

out <- out |> 
  dplyr::left_join(dep_data)

write.csv(out, paste0('tbmu_processed/tbmu_dive_activity.csv'), row.names = F) 
