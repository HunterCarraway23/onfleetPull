#onfleetPull function is used to pull data from the onfleet API and then write it to a database
#according to the dates provided to the 'from' and 'to' variables in the list_tasks function


library(onfleetr)
library(DBI)
library(dplyr)
library(httr)
library(purrr)
library(sf)
library(lubridate)

usethis::edit_r_environ() #edited renviron file for env variables
Sys.getenv("ONFLEET_TOKEN") #checking for api key in renviron
onfleetr::onfleet_auth(api_token = Sys.getenv("ONFLEET_TOKEN")) #authentication test

onfleetPull <- function()
{
  #Pulling data from API for dates: 12-1-21 -> 12-31-21
  dt <- as_datetime("2021-12-01 00:00:01", tz ="America/Los_Angeles")
  dt2 <- as_datetime("2021-12-31 15:59:59", tz ="America/Los_Angeles")

  tib <- onfleetr::list_tasks(from=dt, to=dt2, keep_raw_data = T) #13034 obs with 29 variables

  #chose only deliveries that were successful
  tib <- subset(tib, success==TRUE) #11701 obs

  #need id, timeCreated, completion_time, geography (location1, location2)
  task_table <- tib %>%
    mutate(
      lon = map_dbl(raw_data, function(x) x$destination$location[[1]]),
      lat = map_dbl(raw_data, function(x) x$destination$location[[2]])
    ) %>%
    select(lon,
           lat,
           id,
           created = timeCreated,
           completed = completion_time)

  #need to convert id to integer and prepare df
  taskT <- mutate(task_table, id = row_number())
  taskQ <- subset(taskT, select = -c(lon,lat))

  #exporting dataframe to CSV
  #taskR <- mutate(task_table, id = row_number())
  #write.csv(taskR, "PATH")

  #subsetting df into coordinates
  taskCo <- subset(taskT, select = c("lon","lat"))
  coordinates(taskCo) <- ~lon + lat

  #coercing to sfc object
  taskCo2 <- sf::st_as_sfc(taskCo)
  str(taskCo2)

  #creating hex values from point vector
  taskBin<- sf::st_as_binary(taskCo2)
  class(taskBin)
  raw <- sf::rawToHex(taskBin)
  class(raw)

  #combining df with raw hex data
  n <- dplyr::bind_cols(raw, taskQ)
  colnames(n)[1] = 'geometry'

  #connecting to database using environmental variables
  conn <- dbConnect(RPostgres::Postgres(), dbname=Sys.getenv("DB_NAME"), host=Sys.getenv("DB_HOST"),
                    port=Sys.getenv("DB_PORT"), user=Sys.getenv("DB_USER"), password=Sys.getenv("DB_PASSWORD"))
  hunter <- dbGetQuery(conn, "SELECT * FROM hunter_test") #checking for table in database
  dbAppendTable(conn, "hunter_test", n) #writing data to table
}

dbDisconnect(conn)

