library(tidyverse)
library(geosphere)
library(ggrepel)
# library(ggmap)

setwd("") # Work directory

# Buffer for store radius: 300ft=91.44m

## Top 20 Convenience Stores, grocery stores
# Where they are
# How far they have to travel to get there
# Unique people
## community centers, libraries


## Parameters
BUFFER_GROCERY <- 91.44

## Stores
grocery <- read_csv("./UrbanFarm/dallas_real_grocery.csv")
convenience <- read_csv("./UrbanFarm/dallas_convenience_stores.csv")
dollar <- read_csv("./UrbanFarm/dallas_dollar_stores.csv")

stores <- rbind(grocery, convenience, dollar)
stores <- stores %>%
  rename(Store_ID = `Record ID`,
         store_lon = Longitude,
         store_lat = Latitude) %>%
  select(Store_ID, `Store Type`, store_lon, store_lat)

rm(grocery, convenience, dollar)


## All the Home
ID <- read_csv("/work/group/oit_research_data/mobility/data/data_DFW_2021/data/finalListOfDeviceIDsAtThreeMonthThreshold.txt")
home <- tibble()

for (base_path in list.dirs("/work/group/oit_research_data/mobility/data/data_DFW_2021/data", recursive = F)) {
  for (l in LETTERS[5:11]) {
    for (num in 10:15) {
      path <- paste0(base_path, "/", l, num, "_DFW_output.tsv")
      temp <- read_delim(path, show_col_types = F, progress = F)
      temp <- temp %>% filter(Device_ID %in% ID$Device_ID)
      home <- rbind(home, temp)
    }
  }
}

home <- home[!duplicated(home$Device_ID),]

home <- home %>% 
  rename(home_lat = Lat,
         home_lon = Lon)
rm(ID, temp, l, num, path)


## Process Customer Data
files <- list.files("./UrbanFarm/temp/")
for (i in 1:length(files)) {
  if (i %% 5 == 0) {print(i)}
  temp <- read_csv(paste0("./UrbanFarm/temp/", files[i]), show_col_types = F, progress = F)
  temp <- temp %>%
    filter(Distance <= BUFFER_GROCERY) %>%
    mutate(Hour = hour(DateTime)) %>%
    filter(Hour >= 8, 
           Hour <= 20) %>%
    group_by(Store_ID, Date, Device_ID) %>%
    distinct(Device_ID) %>%
    left_join(stores) %>%
    left_join(home)
  
  write_csv(temp, "./UrbanFarm/customers.csv", append = T)
}

customers <- read_csv("./UrbanFarm/customers.csv", col_names = F)
customers <- customers %>%
  mutate(Distance = distVincentyEllipsoid(as.matrix(customers[, c(5, 6)]),
                                          as.matrix(customers[, c(8, 7)])))
colnames(customers) <- colnames(temp)
write_csv(customers, "./UrbanFarm/customers.csv")

rm(temp, files, i, base_path)

###################
## Find Top Stores
###################

customers <- read_csv("./UrbanFarm/customers.csv")

## Stores
grocery <- read_csv("./UrbanFarm/dallas_real_grocery.csv")
convenience <- read_csv("./UrbanFarm/dallas_convenience_stores.csv")
dollar <- read_csv("./UrbanFarm/dallas_dollar_stores.csv")
# store_names <- read_csv("./UrbanFarm/store_names.csv")
# store_names <- store_names[,-1]

stores <- rbind(grocery, convenience, dollar)
stores <- stores %>% rename(Store_ID = `Record ID`) %>%
  mutate(Brand = str_remove(`Store Name`, "\\ $|\\d+$"),
         Brand = str_remove(Brand, "\\#|\\ $|\\d+$"),
         Brand = str_remove(Brand, "\\ $"),
         Brand = str_to_title(Brand))
stores$Brand[grep("7-eleven|7 eleven", stores$Brand, ignore.case = T)] <- "7-Eleven"
stores$Brand[grep("cvs", stores$Brand, ignore.case = T)] <- "CVS"
stores$Brand[grep("albertson", stores$Brand, ignore.case = T)] <- "Albertson"
stores$Brand[grep("dollartree", stores$Brand, ignore.case = T)] <- "Dollar Tree"
stores$Brand[grep("walmart", stores$Brand, ignore.case = T)] <- "Walmart"

sort(table(stores$Brand))
rm(grocery, convenience, dollar)

## Top Brands
brands <- customers %>%
  left_join(stores) %>%
  group_by(Brand) %>%
  summarise(Traffic = n()) %>%
  arrange(-Traffic)

head(brands, 20)

write_csv(brands, "./UrbanFarm/top/brands.csv")

# rm(brands)

## Top 20 Convenience Stores
top_convenience <- customers %>%
  filter(`Store Type` %in% c("Combination Grocery/Other", "Convenience Store")) %>%
  group_by(Store_ID) %>%
  summarise(People = n()) %>%
  arrange(-People) %>%
  left_join(stores) %>%
  select(-`...1`) %>%
  head(20)

distance <- customers %>%
  filter(Store_ID %in% top_convenience$Store_ID) %>%
  group_by(Store_ID) %>%
  summarise(MedDistance = median(Distance/1600))

top_convenience <- top_convenience %>%
  left_join(distance)

write_csv(top_convenience, "./UrbanFarm/top/convenience20.csv")


## Top 20 Grocery Stores: Total Traffic
top_grocery <- customers %>%
  filter(`Store Type` %in% c("Large Grocery Store", "Supermarket", "Super Store")) %>%
  group_by(Store_ID) %>%
  summarise(People = n()) %>%
  arrange(-People) %>%
  left_join(stores) %>%
  select(-`...1`) %>%
  head(20)

distance <- customers %>%
  filter(Store_ID %in% top_grocery$Store_ID) %>%
  group_by(Store_ID) %>%
  summarise(MedDistance = median(Distance/1600))

top_grocery <- top_grocery %>%
  left_join(distance)

write_csv(top_grocery, "./UrbanFarm/top/grocery20.csv")

plot1 <- top_grocery %>%
  ggplot(aes(x = Longitude, y = Latitude)) +
    geom_point(aes(size = MedDistance, color = People)) +
    labs(title = "Top 20 Grocery Stores",
         size = "Median Dist. Traveled (Mi.)",
         x = "Longitude", y = "Latitude") +
    geom_text_repel(aes(label = Brand)) +
    xlim(min(customers$store_lon), max(customers$store_lon)) +
    ylim(min(customers$store_lat), max(customers$store_lat))

ggsave("./UrbanFarm/plots/top_grocery.jpeg", width = 8, height=6)


### All top stores Combined
top_combined <- top_convenience %>%
  bind_rows(top_grocery) %>%
  arrange(-MedDistance)

plot1 <- top_combined %>%  
  ggplot(aes(x = Longitude, y = Latitude)) +
  geom_point(aes(size = MedDistance, color = `Store Type`)) +
  labs(title = "Top 20 Storess Combined",
       x = "Longitude", y = "Latitude",
       color = "Store Types",
       size = "Median Distance Traveled",
       subtitle = "Top 5 Stores with Largest Distance Marked") +
  geom_text_repel(data = top_combined[1:5,], aes(label = Brand)) +
  xlim(min(customers$store_lon), max(customers$store_lon)) +
  ylim(min(customers$store_lat), max(customers$store_lat))

ggsave("./UrbanFarm/plots/grocery_convenience_combined.jpeg", width = 8, height=6)

#######################
## Distributions
#######################

plot1 <- customers %>%
  group_by(`Store Type`) %>%
  summarise(MedDistance = median(Distance/1600)) %>%
  ggplot(aes(x = reorder(`Store Type`, MedDistance), y = MedDistance)) +
  geom_segment(aes(x=`Store Type`, xend=`Store Type`, y=0, yend=MedDistance)) +
  geom_point( size=5, color="pink") +
  geom_text(aes(y = MedDistance + 0.1, label = round(MedDistance, 1))) +
  coord_flip() + 
  theme_bw() +
  labs(y = "Median Distance Traveled (Mi.)", 
       x = "") +
  theme(axis.text = element_text(size = 15),
        axis.title = element_text(size = 20))

ggsave("./UrbanFarm/plots/store_type_distance.jpeg", width = 13, height=6)

customers %>%
  group_by(`Store Type`) %>%
  ggplot(aes(x = Distance/1600, color = `Store Type`)) +
  geom_density()


# customers %>%
#   group_by(`Store Type`) %>%
#   distinct(Device_ID) %>%
#   summarise(People = n()) %>%
#   arrange(-People) %>%
#   ggplot(aes(x = `Store Type`, y = People)) +
#   geom_segment(aes(x=`Store Type`, xend=`Store Type`, y=0, yend=People)) +
#   geom_point( size=5, color="red", fill=alpha("orange", 0.3), alpha=0.7, shape=21, stroke=2) 
# # geom_col()
