library(momentuHMM)
library(raster)
library(ggplot2)
library(dplyr)
theme_set(theme_light())

dep_data <- readRDS('deployments.RDS')

files <- list.files('tbmu_processed', pattern = 'class', full.names = T)
files <- files[grep('Coats',files)]

out <- data.frame()

for (f in files) {
    dat <- readRDS(f)
    
    temp <- dat |> 
      sf::st_drop_geometry() |> 
      dplyr::group_by(dep_id) |> 
      dplyr::mutate(
        year = as.numeric(strftime(time, '%Y')),
        dtime = as.numeric(difftime(time, min(time), units = 'days')),
        day = floor(dtime) + 1
      ) |> 
      dplyr::group_by(year, dep_id, day) |> 
      dplyr::summarise(
        tot_time = dplyr::n()/(60),
        resting = sum(behaviour == 'Colony')/(60),
        diving = sum(behaviour == 'Diving')/(60),
        flying = sum(behaviour == 'Flying')/(60),
        swimming = sum(behaviour == 'Swimming')/(60),
        resting = ifelse(is.na(resting), 0, resting),
        diving = ifelse(is.na(diving), 0, diving),
        flying = ifelse(is.na(flying), 0, flying),
        swimming = ifelse(is.na(swimming), 0, swimming),
        dee = (32.0*resting) + (532.8*flying) + (100.8*swimming) +(97.2*diving),
        vedba_rest = mean(odba[behaviour == 'Colony']),
        vedba_dive = mean(odba[behaviour == 'Diving']),
        vedba_fly = mean(odba[behaviour == 'Flying']),
        vedba_swim = mean(odba[behaviour == 'Swimming']),
      ) |> 
      dplyr::filter(tot_time > 23)
    
    tt <- temp |> 
      dplyr::left_join(dep_data)
    
    out <- rbind(out, tt)
    
  }

write.csv(out, paste0('tbmu_processed/tbmu_daily_activity.csv'), row.names = F) 
