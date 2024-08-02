library(momentuHMM)
library(raster)
library(ggplot2)
library(dplyr)
theme_set(theme_light())

set.seed(4252)

colony <- 'CGM'
year <- 2023

my_crs <- '+proj=aeqd +lon_0=-82 +lat_0=63 +datum=WGS84 +units=m +no_defs'

dep_data <- readRDS('deployments.RDS') |> 
  dplyr::filter(site == colony)

fn <- sub('.RDS','',list.files('tbmu_data'))

dd <- dep_data |> 
  dplyr::filter(strftime(time_released, '%Y') == year, site == colony) |> 
  dplyr::filter(gps_id != 'L40', !is.na(acc_id)) |>
  dplyr::filter(!(dep_id %in% 
                    c('A21 99699804 20170721', 'G2 99683065 20170802', 'A13 117639807 20180801','A11 99687459 20190703',
                      'Blue11 99687464 20170803','G4 99662921 20170802','A32 99670888 20170727','AA28_99687275_20230720',
                      'A20 118608209 20180722','A110 118608142 20180802','A34 117639756 20180726',
                      'A37 99687474 20180721'))
                ) |>
  dplyr::pull(dep_id)

col_loc <- unique(dep_data[dep_data$dep_id %in% dd, c('site', 'dep_lon', 'dep_lat')])

col_loc <- col_loc |> 
  dplyr::summarize(
    dep_lon = mean(dep_lon),
    dep_lat = mean(dep_lat)
  )

# -----

time_step <- '60 sec'

fn <- list.files(paste0('tbmu_data/'), full.names = T)
fn <- fn[basename(fn) %in% paste0(dd, '.RDS')]
dd <- sub('.RDS', '', (basename(fn)))

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
                    attempts = 100,
                    predTime = pt)

# Examine tracks for any obvious issues in interpolation
# plot(crwOut)

# -----
# merge interpolated locations with processed acc_data
# read in fields from acc_data
data <- do.call(rbind, lapply(paste0('tbmu_data/',dd,'.RDS'), readRDS))

data <- data |> 
  dplyr::filter(!is.na(depth_m)) |> # remove any tracks with missing depth data
  dplyr::filter(dep_id %in% dd) |> 
  dplyr::select(dep_id, time, depth_m, wbf, pitch, odba) 

# summarize acc_data at time-step
sumdata <- data |> 
  dplyr::group_by(dep_id) |> 
  dplyr::mutate(
    wbf = ifelse(wbf < .1, 0, wbf),
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

# for (d in unique(sumdata$ID)) {
#   p <- sumdata |>
#     dplyr::filter(ID == d) |>
#     dplyr::select(ID, time,  wbf, diving, pitch, odba) |>
#     tidyr::pivot_longer(cols = c(wbf, diving, pitch, odba)) |>
#     ggplot(aes(x = time, y = value)) +
#     geom_line() +
#     #geom_point(aes(col = behaviour), size = 0.9) +
#     facet_grid(rows = vars(name), scales = 'free') +
#     labs(x = 'Time', y = '',  title = d)
#   print(p)
#   readline('next')
# }

# merge with interpolated gps data
myData <- crawlMerge(crwOut, sumdata, 'time')

# -----

# col_loc <- sp::SpatialPoints(col_loc,proj4string = sp::CRS('+proj=longlat +ellps=WGS84'))
# col_loc <- sp::spTransform(col_loc, raster::crs(gps_sp))

col_loc <- sf::st_as_sf(col_loc, coords = c('dep_lon', 'dep_lat'), crs = 4326)
col_loc <- sf::st_transform(col_loc, my_crs)

# -----

# best predicted track data
hmmData <- prepData(myData,
                    centers = sf::st_coordinates(col_loc))
hist(hmmData$center1.dist[hmmData$center1.dist < 1000], 100)

#hmmData$wbf[hmmData$wbf < 6] <- 0
#hmmData$wbf <- ifelse(hmmData$wbf == min(hmmData$wbf, na.rm = T), 0, hmmData$wbf)


dist <- list(wbf = "gamma", 
             diving = "beta", 
             #step = 'gamma', 
             pitch = 'norm'#,
             #odba = 'gamma'
)


# extract starting values
m_start <- readRDS('data/tbmu_hmm_startvalues.RDS')
bestPar <- getPar(m_start)
bestPar$Par$diving[10] <- 0.00000000001

beta <- bestPar$beta

# probability of transitioning from diving, flying, and colony depend on distance from the colony
# transFormula <- ~state2(I(center1.dist>=500)) + state3(I(center1.dist>=500)) + state4(I(center1.dist<500))

stateNames <- c('Flying', 'Diving', 'Swimming', 'Colony')
nbStates <- length(stateNames)

st <- Sys.time()
m <- fitHMM(hmmData, 
            #mvnCoords="mu", altCoordNames = "mu",
            nbStates=nbStates, 
            dist=dist, 
            #formula=transFormula,
            Par0=bestPar$Par, 
            beta0=bestPar$beta,
            stateNames = stateNames, #state names
            #knownStates = ifelse(hmmData$diving >= 3/20, 2, NA) # if > 5 seconds of diving set as diving
)
Sys.time() - st
m
#plot(m)

saveRDS(m, paste0('tbmu_processed/TBMU_HMM_', colony, "_",year,'.RDS'))

hmmData$behaviour <- viterbi(m)
hmmData$behaviour <- factor(hmmData$behaviour, labels = stateNames)

# -----

# hmmData$x[hmmData$behaviour == 'Colony' & hmmData$center1.dist < 2000] <- as.matrix(data.frame(col_loc)[,1])
# hmmData$y[hmmData$behaviour == 'Colony' & hmmData$center1.dist < 2000] <- as.matrix(data.frame(col_loc)[,2])

hmm_sf <- hmmData |> 
  dplyr::select(ID, time, diving, wbf, pitch, odba, behaviour, x, y) |> 
  dplyr::rename(dep_id = ID) |> 
  sf::st_as_sf(coords = c('x', 'y'), crs = my_crs) 

saveRDS(hmm_sf, paste0('tbmu_processed/TBMU_classified_', colony, "_",year,'.RDS'))
`
`# -----
# Examine classification

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

hmmData |> 
  group_by(ID, behaviour) |> 
  summarize(
    pitch = mean(pitch)
  ) |> View()

# -----

