library(tidyverse)
library(geosphere)
library(doParallel)
library(foreach)


setwd("/lustre/work/client/users/kevinwang/")

################### Global Parameters
BUFFER_LON <- 0.005
BUFFER_LAT <- 0.005


###### All Stores
grocery <- read_csv("./UrbanFarm/dallas_real_grocery.csv")
convenience <- read_csv("./UrbanFarm/dallas_convenience_stores.csv")
dollar <- read_csv("./UrbanFarm/dallas_dollar_stores.csv")

# We can combine them and store the IDs
grocery <- rbind(grocery, convenience, dollar)
rm(convenience, dollar)


#### Process each Square
process_square <- function(path, mon, grocery) {
  # Load the Data
  base_path <- "/work/group/oit_research_data/mobility/data/data_DFW_2021/data"
  mobility <- read_delim(paste0(base_path, "/",  path),show_col_types = F, progress = F)
  
  # Locate Stores within the grid
  stores_within_grid <- c()
  for (i in 1:nrow(grocery)) {
    if (grocery$Latitude[i] < min(mobility$Lat) | grocery$Latitude[i] > max(mobility$Lat) |
        grocery$Longitude[i] < min(mobility$Lon) | grocery$Longitude[i] > max(mobility$Lon)) {
      next
    }
    stores_within_grid <- c(stores_within_grid, i)
  }
  
  rm(i)
  
  ### Find customers who visted the square
  customers <- data.frame()
  for (i in 1:length(stores_within_grid)) {
    store_lon <- grocery$Longitude[stores_within_grid[i]]
    store_lat <- grocery$Latitude[stores_within_grid[i]]
    
    temp <- mobility %>%
      filter(Lon < store_lon + 0.005,
             Lon > store_lon - 0.005,
             Lat < store_lat + 0.005,
             Lat > store_lat - 0.005) %>%
      mutate(Store_ID = grocery$`Record ID`[stores_within_grid[i]])
    
    temp$Distance <- distVincentyEllipsoid(c(store_lon, store_lat), as.matrix(temp[,c(4,3)]))
    customers <- rbind(customers, temp)
  }
  
  paths <- unlist(strsplit(path, split = "/"))
  grid_ID <- unlist(strsplit(paths[2], split = "_"))[1]
  
  write_csv(customers, paste0("/lustre/work/client/users/kevinwang/UrbanFarm/temp/", paths[1], "_", grid_ID, ".csv"))
  print(paste0("./UrbanFarm/temp/", paths[1], "_", grid_ID, ".csv"))
}


## Find Grid ID
path <- c()
base_path <- "/work/group/oit_research_data/mobility/data/data_DFW_2021/data"
all_months <- list.dirs(base_path, recursive = F, full.names = F)
for (mon in all_months[1:12]) {
  for (l in LETTERS[5:9]) {
    for (num in 10:15) {
      path <- c(path, paste0(mon, "/", l, num, "_DFW_mobility_output.tsv"))
    }
  }
}


registerDoParallel(16)
foreach(id=path, .packages = c("geosphere", "tidyverse")) %dopar% {
  process_square(id, mon, grocery)
}
stopImplicitCluster()



########################
## Fast Food
########################
fast_food <- read_csv("./UrbanFarm/Dallas_County_Top_Ten_Fast_Food_Locations.csv")
fast_food <- fast_food %>%
  rename(`Record ID` = LOCNUM,
         Longitude = LON, 
         Latitude = LAT) %>%
  filter(!is.na(Latitude))

#### Process each Square
process_square <- function(path, grocery) {
  # Load the Data
  base_path <- "/work/group/oit_research_data/mobility/data/data_DFW_2021/data"
  mobility <- read_delim(paste0(base_path, "/",  path),show_col_types = F, progress = F)
  
  # Locate Stores within the grid
  stores_within_grid <- c()
  for (i in 1:nrow(grocery)) {
    if (grocery$Latitude[i] < min(mobility$Lat) | grocery$Latitude[i] > max(mobility$Lat) |
        grocery$Longitude[i] < min(mobility$Lon) | grocery$Longitude[i] > max(mobility$Lon)) {
      next
    }
    stores_within_grid <- c(stores_within_grid, i)
  }
  
  rm(i)
  
  if (is.null(stores_within_grid)) {return(NULL)}
  
  ### Find customers who visited the square
  customers <- data.frame()
  for (i in 1:length(stores_within_grid)) {
    store_lon <- grocery$Longitude[stores_within_grid[i]]
    store_lat <- grocery$Latitude[stores_within_grid[i]]
    
    temp <- mobility %>%
      filter(Lon < store_lon + 0.005,
             Lon > store_lon - 0.005,
             Lat < store_lat + 0.005,
             Lat > store_lat - 0.005) %>%
      mutate(Store_ID = grocery$`Record ID`[stores_within_grid[i]])
    
    temp$Distance <- distVincentyEllipsoid(c(store_lon, store_lat), as.matrix(temp[,c(4,3)]))
    customers <- rbind(customers, temp)
  }
  
  paths <- unlist(strsplit(path, split = "/"))
  grid_ID <- unlist(strsplit(paths[2], split = "_"))[1]
  
  write_csv(customers, paste0("/lustre/work/client/users/kevinwang/UrbanFarm/temp/fast_food/", paths[1], "_", grid_ID, ".csv"))
  print(paste0("./UrbanFarm/temp/fast_food/", paths[1], "_", grid_ID, ".csv"))
}


## Find Grid ID
path <- c()
base_path <- "/work/group/oit_research_data/mobility/data/data_DFW_2021/data"
all_months <- list.dirs(base_path, recursive = F, full.names = F)
for (mon in all_months[1:12]) {
  for (l in LETTERS[5:9]) {
    for (num in 10:15) {
      path <- c(path, paste0(mon, "/", l, num, "_DFW_mobility_output.tsv"))
    }
  }
}


registerDoParallel(16)
foreach(id=path, .packages = c("geosphere", "tidyverse")) %dopar% {
  process_square(id, fast_food)
}
stopImplicitCluster()
