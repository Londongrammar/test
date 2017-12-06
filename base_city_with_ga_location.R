setwd("/home/ubuntu/Conversion/Vikalp/Base_City")
### load libraries 
library("DBI")
library('rJava')

library(doBy)
library(stringr)
library(BBmisc)
library(dplyr)
library(RGoogleAnalytics)
detach("package:vegan", unload=TRUE)

library(RPostgreSQL)
library(RGA)
library(data.table)
library('assertthat')
library('bigrquery')

#### end loading 
  ### connection 
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname="oyoprod",host="prod-read-replica-analytics.czukitb4ckf9.ap-southeast-1.rds.amazonaws.com",
                 port=5432,user="analytics_read",
                 password="@NALyT1csWr1T$" )


ana<-dbConnect(drv, dbname="analytics",host="oyo-analytics.czukitb4ckf9.ap-southeast-1.rds.amazonaws.com",
               port=5432,user="analytics",
               password="analytics123")



####
#### get the last 90 days user geo location data from bigquery

library('assertthat')
library('bigrquery')
library(doBy)
library(dplyr)

project = "bigquery01-1281"
library('httr')
ga_geo_location<-data.frame()
for(i in 0:2)
{
  b = paste("'",Sys.Date() - 2-i*31,"'",sep="")
  a = paste("'",Sys.Date() - 2-i*30-30,"'",sep="")
  print(a)
  print(b)

m = paste("select month(date) as month, year(date) as year, guest_id,city,region, count(*) as total_num, 
          max(date) as last_browsed_date from (select a.date as date, a.geoNetwork.city as city,
          a.geoNetwork.region as region,hits.customDimensions.value as guest_id from 
          ( select fullVisitorID, date, geoNetwork.city,geoNetwork.region FROM 
          (TABLE_DATE_RANGE([101018286.ga_sessions_], TIMESTAMP(",a,"),TIMESTAMP(",b,"))) ) as a 
          join ( select fullVisitorID, hits.customDimensions.value from 
          (TABLE_DATE_RANGE([101018286.ga_sessions_],TIMESTAMP(",a,"),TIMESTAMP(",b,"))) 
          where hits.customDimensions.index = 48 and hits.customDimensions.value not in ('0') 
          and hits.customDimensions.value is not null group by fullVisitorID, hits.customDimensions.value ) as b 
          on a.fullVisitorID = b.fullVisitorID group by date,city,region,guest_id) as a

          group by month, year, guest_id,city,region;", sep = "")
temp <- query_exec(m, project = project, max_pages = Inf)
ga_geo_location<-rbind(ga_geo_location,temp)
}
ga_geo_location$length = nchar(ga_geo_location$city)
ga_geo_location = subset(ga_geo_location,length > 2)
ga_geo_location = ga_geo_location[,c(1:7)]

ga_geo_location1 = subset(ga_geo_location,region == 'Delhi' & city == '(not set)')[,c(1:3,5:7)]
names(ga_geo_location1)[4] = 'city'
ga_geo_location2 = subset(ga_geo_location, city!= '(not set)')[,c(1:4,6,7)]
ga_geo_location3<-rbind(ga_geo_location1,ga_geo_location2)

### check the users at city level 
city_region = unique(subset(ga_geo_location, city!= '(not set)')[,c(3,4,5)])
city_region$length = nchar(city_region$city)
city_region = subset(city_region,length >= 2)

options(sqldf.driver = "SQLite")
library(sqldf)

city_region = sqldf(" select city, region, count(distinct guest_id) from 
                    city_region group by city, region order by count(distinct guest_id) desc")

#####
##### convert the city names as oyo city names eg: New Delhi to delhi 

ga_geo_location3$new_city = ifelse(ga_geo_location3$city == 'New Delhi', 'Delhi', ifelse(ga_geo_location3$city == 'Bengaluru','Bangalore',
                                                                                         ifelse(ga_geo_location3$city == 'Navi Mumbai','Mumbai',
                                                                                                ifelse(ga_geo_location3$city == 'Gurugram','Gurgaon',
                                                                                                       ifelse(ga_geo_location3$city == 'Mira Bhayandar','Mumbai',
                                                                                                              ifelse(ga_geo_location3$city == 'Thane','Mumbai',
                                                                                                                     ifelse(ga_geo_location3$city == 'Kalyan','Mumbai',
                                                                                                                            ifelse(ga_geo_location3$city == 'Dombivli','Mumbai',
                                                                                                                                   ifelse(ga_geo_location3$city == 'Ulhasnagar','Mumbai',
                                                                                                                                          ifelse(ga_geo_location3$city == 'Vasai','Mumbai',
                                                                                                                                                 ifelse(ga_geo_location3$city == 'Virar','Mumbai',
                                                                                                                                                        ifelse(ga_geo_location3$city == 'Pimpri-Chinchwad','Pune',
                                                                                                                                                               ifelse(ga_geo_location3$city == 'Secunderabad','Hyderabad',
                                                                                                                                                                      ifelse(ga_geo_location3$city == 'Greater Noida','Noida',
                                                                                                                                                                             ifelse(ga_geo_location3$city == 'Kozhikode','Calicut',
                                                                                                                                                                                    ifelse(ga_geo_location3$city == 'Mysuru','Mysore',
                                                                                                                                                                                           ifelse(ga_geo_location3$city == 'Mangaluru','Mangalore',
                                                                                                                                                                                                  ifelse(ga_geo_location3$city == 'Hubballi','Hubli',
                                                                                                                                                                                                         ifelse(ga_geo_location3$city == 'Belagavi','Belgaum',
                                                                                                                                                                                                                ifelse(ga_geo_location3$city == 'Sahibzada Ajit Singh Nagar','Chandigarh',
                                                                                                                                                                                                                       ifelse(ga_geo_location3$city == 'Howrah','Kolkata',
                                                                                                                                                                                                                              ifelse(ga_geo_location3$city == 'Panjim','Goa',
                                                                                                                                                                                                                                     ifelse(ga_geo_location3$city == 'Cochin','Kochi',
                                                                                                                                                                                                                                            ifelse(ga_geo_location3$city == 'Bokaro Steel City','Bokaro',
                                                                                                                                                                                                                                                   ifelse(ga_geo_location3$city == 'Puducherry','Pondicherry',ga_geo_location3$city   )))))))))))))))))))))))))


ga_geo_location3$new_city<-sapply(ga_geo_location3$new_city,tolower)

### user frrquent city 
#f1<-function(x){sum(x)}
user_city_freq<- ga_geo_location3 %>% dplyr::group_by(guest_id,new_city) %>% dplyr::summarise(count=sum(total_num))
#user_city_freq<-summaryBy(total_num~guest_id+new_city,data=ga_geo_location3,FUN=f1)

user_city_freq<-user_city_freq[order(user_city_freq$count,decreasing = TRUE),]
#colnames(user_city_freq)<-c("guest_id","city","count")
user_city_new<-user_city_freq[!duplicated(user_city_freq$guest_id),]
user_city_new<-user_city_new[user_city_new$count>=5,]
### update data for new users & base city update 

### get current user base city 

current_bc<-dbGetQuery(conn = ana,
statement = paste0("select * from new_base_city_v3"))
user_data<-merge(user_city_new,current_bc,by.x="guest_id",by.y="user_id",all=TRUE)

## oyo cities 
cities<-data.frame(dbGetQuery(conn = con,
                              statement = paste0("select c.name as city from cities c")))
cities$city<-sapply(cities$city,tolower)
cities$kon<-1

## users hows base city has changed

user_data1<-user_data[!is.na(user_data$new_city),]

user_data2<-user_data1[!is.na(user_data1$base_city),]

user_data3<-user_data2[user_data2$new_city!=user_data2$base_city,]

base_city_update<-merge(user_data3,cities,by.x="new_city",by.y="city",all.x=TRUE)

base_city_update$kon<-ifelse(is.na(base_city_update$kon),0,1)

data1<-base_city_update[c(2,1,8,7)]
data1[4]<-Sys.Date()
colnames(data1)<-c("user_id","base_city","oyo_city","updated_at")

### new users 

user_data4<-user_data1[is.na(user_data1$base_city),]

base_city_new<-merge(user_data4,cities,by.x="new_city",by.y="city",all.x=TRUE)

base_city_new$kon<-ifelse(is.na(base_city_new$kon),0,1)

data2<-base_city_new[c(2,1,8,7)]
data2[4]<-Sys.Date()
colnames(data2)<-c("user_id","base_city","oyo_city","updated_at")

## old users no data no update
user_data5<-user_data[!is.na(user_data$base_city),]
user_data6<-user_data5[is.na(user_data5$new_city),]
data3<-user_data6[c(1,5,6,7)]
colnames(data3)<-c("user_id","base_city","oyo_city","updated_at")

## old users data no update
user_data7<-user_data2[user_data2$new_city!=user_data2$base_city,]
data4<-user_data7[c(1,5,6,7)]
colnames(data4)<-c("user_id","base_city","oyo_city","updated_at")


### add all data 
user_base_city_final<-rbind(data1,data2,data3,data4)


## test
users<-data.frame(c('1055793','1207893','23393','9699279','3993363','77035','6176946','3445099','11962738','4707863','31169','2835535','2251660','16088735','12901','5185111','5067761','32495','14152268','3993349','1704741'))
colnames(users)<-"id"


user_base_city_final$user_id<-as.numeric(user_base_city_final$user_id)

f1<- function (x) {sub("\\s+$", "", x)}    ## remove spaces from the end 
user_base_city_final$base_city<- f1(user_base_city_final$base_city)  ### 

## users whose base city not available 
# user_input_base_city<-data.frame(dbGetQuery(conn = con,
#                               statement = paste0("select id,city from user_profiles where city !='' 
# ")))
# user_input_base_city$city<-sapply(user_input_base_city$city,tolower)
# 
# user_base_city_final<-data.frame(dbGetQuery(conn = ana,
#                                             statement = paste0("select * from new_base_city_v4 
# ")))
# 
# 
#   all_users<-merge(user_base_city_final,user_input_base_city,by.x="user_id",by.y="id",all=TRUE)
# users_with_no_base_city<-all_users[is.na(all_users$base_city) & !is.na(all_users$city),]
# users_with_no_base_city1<-merge(users_with_no_base_city,cities,by.x="city",by.y="city",all.x=TRUE)
# 
# 
## write to DB
dbWriteTable(ana, value = data.frame(user_base_city_final), name = "new_base_city_v4", append = TRUE, header = FALSE, row.names=FALSE, overwrite=FALSE)

