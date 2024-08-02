library(momentuHMM)
library(raster)
library(ggplot2)
library(dplyr)
theme_set(theme_light())

set.seed(654)

my_crs <- '+proj=aeqd +lon_0=-82 +lat_0=63 +datum=WGS84 +units=m +no_defs'

dep_data <- readRDS('deployments.RDS') |> 
  dplyr::filter(site == 'Coats' & acc_id != 'L40')


fn <- sub('.RDS','',list.files('tbmu_data'))

dd <- sample(fn, 30)
# col_loc <- unique(dep_data[dep_data$dep_id %in% dd, c('site', 'dep_lon', 'dep_lat')])
# col_loc <- col_loc |> 
#   summarize(
#     dep_lon = mean(dep_lon),
#     dep_lat = mean(dep_lat)
#   )

my_crs <- paste0('+proj=aeqd +lon_0=',col_loc$dep_lon,' +lat_0=',col_loc$dep_lat,' +datum=WGS84 +units=m +no_defs')

# -----

time_step <- '60 sec'

# read in raw gps data
gps <- arrow::open_dataset('raw_data/gps') |> 
  dplyr::filter(deployed == 1, dep_id %in% dd) |>
  dplyr::select(dep_id, time, lon, lat) |> 
  dplyr::collect() |> 
  dplyr::group_by(dep_id) |> 
  dplyr::arrange(dep_id, time) |> 
  dplyr::filter(dep_id %in% dep_data$dep_id)

# filter out unrealistic speeds > 100 km/hr
gps <-do.call(rbind, lapply(unique(gps$dep_id), function(x) {
  seabiRds::filterSpeed(data.frame(gps[gps$dep_id == x,]), threshold = 100)
}
))

col_loc <- unique(dep_data[dep_data$dep_id %in% dd, c('site', 'dep_lon', 'dep_lat')])
col_loc <- gps |> 
  dplyr::filter(dist < 0.05) |> 
  dplyr::summarize(
    dep_lon = seabiRds::getMode(round(lon, 4)),
    dep_lat = seabiRds::getMode(round(lat, 4))
  )

my_crs <- paste0('+proj=aeqd +lon_0=',col_loc$dep_lon,' +lat_0=',col_loc$dep_lat,' +datum=WGS84 +units=m +no_defs')


# # convert to a projected coordinate system
# gps_sp <- sp::SpatialPointsDataFrame(coords = gps[,c('lon', 'lat')], data = gps, proj4string = sp::CRS('+proj=longlat'))
# gps_sp <- sp::spTransform(gps_sp, CRS = 'EPSG:3346')

gps_sf <- sf::st_as_sf(gps, coords = c('lon', 'lat'), crs = 4326)
gps_sf <- sf::st_transform(gps_sf, my_crs)

# generate a list of prediction times for each individual in gps
predTime <- gps |> 
  dplyr::group_by(dep_id) |> 
  dplyr::summarize(
    start = lubridate::round_date(min(time), time_step),
    end = lubridate::round_date(max(time), time_step))
pt <- lapply(1:nrow(predTime), function(x) seq.POSIXt(predTime$start[x], predTime$end[x], time_step))
names(pt) <- predTime$dep_id

# Use crawl to interpolate gps data at time-stemp
lnError <- crawl::argosDiag2Cov(50,50,0) # 50m isotropic error ellipse
crawlData <- data.frame(ID = gps$dep_id,
                          time = gps$time,
                          x = sf::st_coordinates(gps_sf)[,1],#gps_sp@coords[, 1],
                          y = sf::st_coordinates(gps_sf)[,2],#gps_sp@coords[, 2],
                          ln.sd.x = lnError$ln.sd.x,
                          ln.sd.y = lnError$ln.sd.y,
                          error.corr = lnError$error.corr)
crwOut <- crawlWrap(crawlData,
                    theta = c(6.5,-.1),
                    fixPar = c(1,1,NA,NA),
                    err.model = list(x = ~ln.sd.x-1,
                                     y = ~ln.sd.y-1,
                                     rho = ~error.corr),
                    #timeStep = '1 min', # predict at 15 min time steps
                    attempts = 100,
                    predTime = pt)

# Examine tracks for any obvious issues in interpolation
# plot(crwOut)

# -----
# merge interpolated locations with processed acc_data

# read in fields from acc_data
data <- do.call(rbind, lapply(paste0('tbmu_data/',dd, '.RDS'), readRDS))

data <- data |> 
  dplyr::filter(!is.na(depth_m)) |> # remove any tracks with missing depth data
  dplyr::filter(dep_id %in% dd) |> 
  dplyr::select(dep_id, time, depth_m, wbf, pitch, odba) 

# summarize acc_data at time-step
sumdata <- data |> 
  dplyr::mutate(
    time = lubridate::round_date(time, unit = time_step)
  ) |> 
  dplyr::group_by(dep_id, time) |> 
  dplyr::summarize(
    diving = sum(depth_m > 1)/dplyr::n(), # time diving during time step
    wbf = median(wbf, na.rm = T), 
    pitch = median(pitch, na.rm = T), 
    odba = median(odba, na.rm = T), 
  ) |> 
  dplyr::rename(ID = dep_id)

summary(sumdata$wbf)

# merge with interpolated gps data
myData <- crawlMerge(crwOut, sumdata, 'time')

# -----

# col_loc <- sp::SpatialPoints(col_loc, proj4string = sp::CRS('+proj=longlat +ellps=WGS84'))
# col_loc <- sp::spTransform(col_loc, raster::crs(gps_sp))

col_loc <- sf::st_as_sf(col_loc, coords = c('dep_lon', 'dep_lat'), crs = 4326)
col_loc <- sf::st_transform(col_loc, my_crs)

# -----

# best predicted track data
hmmData <- prepData(myData,
                    centers = sf::st_coordinates(col_loc))
#centers = as.matrix(data.frame(col_loc)[,1:2]))

hist(hmmData$`1.dist`[hmmData$`1.dist` < 5000], 100)

hmmData$wbf <- ifelse(hmmData$wbf == min(hmmData$wbf, na.rm = T), 0, hmmData$wbf)

stateNames <- c('Flying', 'Diving', 'Swimming', 'Resting')
nbStates <- length(stateNames)

dist <- list(wbf = "gamma", 
             diving = "beta", 
             #step = 'gamma', 
             pitch = 'norm',
             odba = 'gamma'
             )

wbfPar <- c(8, 1, 0.1, 0.1, # mean of wbf for each state
            1, 1, 0.1, 0.1, # sd of wbf for each state 
            0.01, 0.9, 0.9, 0.9
           ) 
divingPar <- c(0.01, 10, 0.01, 0.01, # beta shape1 parameter for each state
               100, 10, 100, 100, # beta shape2 parameter for each state
               0.999, 0.0001, 0.999, 0.999, # probability of a 0 in each state
               0.0001, 0.999, 0.0001, 0.0001) # probability of a 1 in each state
pitchPar <- c(0, 0, -5, 45, # mean of pitch for each state
              10, 50, 5, 25) # sd of pitch for each state
stepPar <- c(500, 50, 50, 10, # mean of step for each state
             500, 50, 50, 10) # sd of step for each state

odbaPar <- c(1, 0.25, 0.25, 0.05, # mean of step for each state
             0.1, 0.1, 0.1, 0.05) # sd of step for each state

# probability of transitioning from diving, flying, and colony depend on distance from the colony
#transFormula <- ~state2(I(center1.dist>=500)) + state3(I(center1.dist>=500)) + state4(I(center1.dist<500))
# stepDM <- list(mean = distFormula, sd = distFormula)
# DM <- list(step = stepDM)

beta <- matrix(c(NA, NA, NA,
         NA, NA, -100,
         NA, NA, -100,
         NA, -100, -100), nrow = 1, byrow = T)

st <- Sys.time()
m <- fitHMM(
  data = hmmData, # data
  nbStates = nbStates, # state names
  dist = dist, # state distributions
  #formula = transFormula, # formula for transitions
  Par0 = list(wbf = wbfPar, diving = divingPar, step = stepPar, pitch = pitchPar, odba = odbaPar), # starting values
  #DM = DM,
  beta0 = beta,
  stateNames = stateNames, #state names
  knownStates = ifelse(hmmData$diving >= 5/60, 2, NA) # if > 5 seconds of diving set as diving
)
Sys.time() - st

m
plot(m)

if (dir.exists('data') == FALSE) dir.create('data')
#saveRDS(m, 'data/tbmu_hmm_startvalues.RDS')

# -----

# check number of observations by state and dep_id
table(hmmData$ID, viterbi(m))

# predict states
hmmData$behaviour <- viterbi(m)
hmmData$behaviour <- factor(hmmData$behaviour, labels = stateNames)

# boxplot of wbf by state
ggplot(hmmData, aes(x = behaviour, y = wbf)) +
  geom_boxplot()

# boxplot of diving by state
ggplot(hmmData, aes(x = behaviour, y = diving)) +
  geom_boxplot()

# boxplot of distance from colony by state
ggplot(hmmData, aes(x = behaviour, y = center1.dist)) +
  geom_boxplot()

# boxplot of pitch by state
ggplot(hmmData, aes(x = behaviour, y = pitch)) +
  geom_boxplot()

# boxplot of odba by state
ggplot(hmmData, aes(x = behaviour, y = odba)) +
  geom_boxplot()

for (d in unique(hmmData$ID)) {
  p <- hmmData |> 
    dplyr::filter(ID == d) |> 
    dplyr::select(ID, time, behaviour, `center1.dist`, wbf, diving, pitch, odba) |> 
    tidyr::pivot_longer(cols = c(`center1.dist`, wbf, diving, pitch, odba)) |> 
    ggplot(aes(x = time, y = value)) +
    geom_line() +
    geom_point(aes(col = behaviour), size = 0.9) +
    facet_grid(rows = vars(name), scales = 'free') +
    labs(x = 'Time', y = '', col = 'behaviour', title = d)
  print(p)
  readline('next')
}

