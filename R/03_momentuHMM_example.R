library(momentuHMM)
library(dplyr)

dep <- readRDS("raw_data/deployments-PEBO.RDS")

d <- readRDS('processed_data/acc_data_PEBO.RDS') |> 
  dplyr::select(dep_id, time, lon, lat, coldist, wbf, depth_m) |> 
  rename(ID = dep_id) |> 
  data.frame() |> 
  prepData(type = "LL", coordNames = c("lon", "lat")) |> 
  mutate(
    colony = ifelse(coldist < 1, 1, 0), #could be parameter mindist
    wbf = ifelse(wbf < 2, 0, wbf),
    diving = ifelse(depth_m > 0.5, 1, 0)#could be paramerte mindepth
  )


#set model parameters
stateNames <- c("Flying", "Foraging", "Colony", "Resting") #, 'Resting' ADD A FORAGING 2, for 
nbStates <- length(stateNames)

wbfDist <- "gamma" # step distribution
wbfPar <- c(mean = c(4, 4, 0.01, 0.01), 
            sd = c(0.50, 1, 0.01, 0.01), 
            c(0.000001,0.001,0.99, 0.99) #maybe be careful with other birds 
) #  zero-mass included

colonyDist <- 'bern'
colonyPar <- c(0.1, 0.1, 0.99999, 0.0000000001)

diveDist <- 'bern'
divePar <- c(0.0000000001, 0.999999999, 0.0000000001, 0.0000000001)

m <- fitHMM(data=d,
            nbStates=nbStates,
            dist=list(wbf = wbfDist, colony = colonyDist, diving = diveDist),
            Par0=list(wbf = wbfPar, colony = colonyPar, diving = divePar),
            formula = ~ 1
) ## run as a population

d$HMM <- viterbi(m)
d$HMM <- factor(d$HMM, labels = stateNames)

saveRDS(m, 'processed_data/PBO_HMM.RDS')
saveRDS(d, 'processed_data/PBO_HMM_classified.RDS')
#write.csv(birds3, paste0(out_dir, '/', paste0(my.args[1],"_",my.args[2],"_", my.args[3]),'.csv'), row.names = F)

for (i in unique(d$ID)){
  
  bp <- d %>%
    filter(ID == i) %>% 
    dplyr::select(time, coldist, wbf, depth_m, HMM) %>% 
    tidyr::pivot_longer(cols = c('coldist', 'wbf', 'depth_m')) %>% 
    ggplot2::ggplot(ggplot2::aes(x = HMM, y = value)) +
    ggplot2::geom_boxplot() +
    ggplot2::facet_grid(rows = ggplot2::vars(name), scales = 'free') +
    ggplot2::theme(text = element_text(size = 10)) +
    ggtitle(i)
  
  
  tp <- d %>% 
    filter(ID == i) %>% 
    dplyr::select(time, coldist, wbf, depth_m,HMM) %>% 
    tidyr::pivot_longer(cols = c('coldist', 'wbf', 'depth_m' )) %>% 
    ggplot2::ggplot(ggplot2::aes(x = time, y = value)) +
    ggplot2::geom_line() +
    ggplot2::geom_point(ggplot2::aes(col = HMM)) +
    ggplot2::facet_grid(rows = ggplot2::vars(name), scales = 'free') +
    ggplot2::theme(legend.position = c(0.10, 0.90),
                   legend.background = element_blank(),
                   legend.key = element_blank(),
                   text = element_text(size = 10)) +
    ggtitle(i)
  
  p <- cowplot::plot_grid(bp, tp)
  print(p)
  readline('next')
  # ggsave(paste0(out_dir, '/', i,'_plots.png'), p, units = 'in', width = 10, height = 5)
  # 
  # rm(m)
  # print(paste("Finished", i, "at", format(Sys.time(), "%T")))
  
}
