library(momentuHMM)
library(raster)
library(ggplot2)
theme_set(theme_light())

my_crs <- '+proj=eqdc +lon_0=-82 +lat_1=62.3333333 +lat_2=63.6666667 +lat_0=63 +datum=WGS84 +units=m +no_defs'

dep_data <- readRDS('raw_data/deployments.RDS') |> 
  dplyr::filter(site == 'Coats')

col_loc <- unique(dep_data[, c('site', 'dep_lon', 'dep_lat')])

# -----

time_step <- '1 min'

# read in raw gps data
gps <- arrow::open_dataset('raw_data/gps') |> 
  dplyr::filter(deployed == 1) |>
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

# convert to a projected coordinate system
gps_sp <- sp::SpatialPointsDataFrame(coords = gps[,c('lon', 'lat')], data = gps, proj4string = sp::CRS('+proj=longlat'))
gps_sp <- sp::spTransform(gps_sp, CRS = sp::CRS(my_crs))

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
                          x = gps_sp@coords[, 1],
                          y = gps_sp@coords[, 2],
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
                    attempts = 10,
                    predTime = pt)

# Examine tracks for any obvious issues in interpolation
# plot(crwOut)

# -----
# merge interpolated locations with processed acc_data

# read in fields from acc_data
data <- readRDS('processed_data/acc_data.RDS') |> 
  dplyr::filter(!is.na(depth_m)) |> # remove any tracks with missing depth data
  dplyr::filter(dep_id %in% dep_data$dep_id) |> 
  dplyr::select(dep_id, time, depth_m, wbf, pitch) 

# summarize acc_data at time-step
sumdata <- data |> 
  dplyr::mutate(
    time = lubridate::round_date(time, unit = time_step)
  ) |> 
  dplyr::group_by(dep_id, time) |> 
  dplyr::summarize(
    diving = sum(depth_m > 3)/dplyr::n(), # time diving during time step
    wbf = median(wbf, na.rm = T), 
    pitch = median(pitch, na.rm = T), 
  ) |> 
  dplyr::rename(ID = dep_id)

# merge with interpolated gps data
myData <- crawlMerge(crwOut, sumdata, 'time')

# -----

col_loc <- sp::SpatialPoints(col_loc[,2:3],proj4string = sp::CRS('+proj=longlat +ellps=WGS84'))
col_loc <- sp::spTransform(col_loc, raster::crs(gps_sp))

# -----

# best predicted track data
hmmData <- prepData(myData,
                    centers = as.matrix(data.frame(col_loc)))
hmmData <- subset(hmmData, !is.na(hmmData$step))

#hmmData$wbf[hmmData$wbf < 6] <- 0

stateNames <- c('Flying', 'Diving', 'Swimming', 'Colony')
nbStates <- length(stateNames)

dist <- list(wbf = "gamma", diving = "beta", step = 'gamma', pitch = 'norm')

wbfPar <- c(8, 1, 1, 1, # mean of wbf for each state
            1, 1, 1, 1 # sd of wbf for each state 
           ) 
divingPar <- c(0.01, 3, 0.01, 0.01, # beta shape1 parameter for each state
               3, 3, 3, 3, # beta shape2 parameter for each state
               0.9999, 0.0000001, 0.9999, 0.9999, # probability of a 0 in each state
               0.00001, 0.999999, 0.00001, 0.00001) # probability of a 1 in each state
pitchPar <- c(0, 0, -5, 45, # mean of pitch for each state
              10, 50, 5, 25) # sd of pitch for each state
stepPar <- c(500, 50, 50, 10, # mean of step for each state
             500, 50, 50, 10) # sd of step for each state

# # step mean and sd for diving, flying, and colony depend on distance from the colony
distFormula <- ~state2(I(center1.dist>=100)) + state3(I(center1.dist>=100)) + state4(I(center1.dist<100))
# stepDM <- list(mean = distFormula, sd = distFormula)
# DM <- list(step = stepDM)

# probability of transitioning from diving, flying, and colony depend on distance from the colony
transFormula <- distFormula

st <- Sys.time()
m <- fitHMM(
  data = hmmData, # data
  nbStates = nbStates, # state names
  dist = dist, # state distributions
  formula = transFormula, # formula for transitions
  Par0 = list(wbf = wbfPar, diving = divingPar, step = stepPar, pitch = pitchPar), # starting values
  #DM = DM,
  stateNames = stateNames #state names
)
Sys.time() - st

m
plot(m)

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

for (d in unique(hmmData$ID)) {
  p <- hmmData |> 
    dplyr::filter(ID == d) |> 
    dplyr::select(ID, time, behaviour, center1.dist, wbf, diving) |> 
    tidyr::pivot_longer(cols = c(center1.dist, wbf, diving)) |> 
    ggplot(aes(x = time, y = value)) +
    geom_line() +
    geom_point(aes(col = behaviour), size = 0.9) +
    facet_grid(rows = vars(name), scales = 'free') +
    labs(x = 'Time', y = '', col = 'behaviour', title = d)
  print(p)
  readline('next')
  
}

# -----
# Fitting the same model with multiple imputation, not sure it is super useful

# extract starting values
bestPar <- getPar(m)

st <- Sys.time()
miFits <- MIfitHMM(myData, nSims=5,
                   centers = as.matrix(data.frame(col_loc)),
                   #mvnCoords="mu", altCoordNames = "mu",
                   nbStates=nbStates, 
                   dist=dist, 
                   formula=transFormula,
                   Par0=bestPar$Par, 
                   beta0=bestPar$beta,
                   stateNames = stateNames,
                   optMethod = "Nelder-Mead",
                   control = list(maxit=100000))
Sys.time() - st
miFits
plot(miFits)
