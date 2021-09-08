library("jsonlite")
library("mongolite")
library("stringr")


#store the data without cleaning
m <- mongo(collection = "mycol",  db = "mydb", url = "mongodb://localhost")

#we created a file called files_list.txt that contains the paths to all the json files
files <- read.table("files_list.txt", header = TRUE, sep="\n", stringsAsFactors = FALSE)

#we store the data before cleaning into a collection in order to search them and 
#decide what kind of cleaning we will do and if there is need to
data <- c()
for (i in 1:nrow(files)) {
  fj1 <- fromJSON(readLines(files[i,], warn=FALSE, encoding="UTF-8")) # convert from JSON
  tj1 <- toJSON(fj1, auto_unbox = TRUE)  #convert to Json
  data <- c(data, tj1) #add the data in data structure
}

m$insert(data) # insert the data into collection mycol_2 in mongodb

m$distinct("ad_data.Price") # see the distinct values of Price

#store the final data
m2 <- mongo(collection = "mycol_2",  db = "mydb", url = "mongodb://localhost")


data <- c()
for (i in 1:nrow(files)) {
  fj <- fromJSON(readLines(files[i,], warn=FALSE, encoding="UTF-8")) # convert from JSON
  #Data cleaning

  #if the price is not available (value Askforprice) then we put null in there.Also if the price is 1 we also put null beacuse in car.gr 
  #when the price is 1 then you have to call the dealer to find the price, ergo the 1 euro price is not accurate
  #Then we transform string fields with numberic values into numeric fields while also remove the string value that indicates the measurements(ex km, cc)
  if (fj$ad_data$Price == 'Askforprice') {
    fj$ad_data$Price <- NULL
  } else {
    fj$ad_data$Price <- as.numeric(gsub("[\200.]", "", fj$ad_data$Price))     # convert price into numeric and remove euro sign
    #if the price is bellow 12 euros (an unlikely  price for a motorcycle) then assign null value
    if (fj$ad_data$Price < 12) {
      fj$ad_data$Price <- NULL
    }
  }
    
  fj$ad_data$Mileage<- as.numeric(gsub("[,km]", "", fj$ad_data$Mileage)) # convert MIleage into numeric and remove km sign

  fj$ad_data$`Cubic capacity`<- as.numeric(gsub("[,cc]", "", fj$ad_data$`Cubic capacity`))# convert Cupid Capacity into numeric and remove cc sign
  fj$ad_data$Power <- as.numeric(gsub("[,bhp]", "", fj$ad_data$Power)) # convert Power into numeric and remove bhp sign

  tj <- toJSON(fj, auto_unbox = TRUE)  #convert to Json
  data <- c(data, tj) #add the data in data structure
}

m2$insert(data) # insert the data into collection mycol_2 in mongodb.That collection has the final cleaned data


# Question 2
m2$count() #count the files in our collection and so find the number of motorcycles

# Question 3 
# using aggregate we calculate the average price and the sum by adding 1 for every record retrieve
m2$aggregate('[{ 
             "$match" : {"ad_data.Price" : 
             { "$exists" : true } }},
             {"$group":{"_id": "ad_id", 
             "Average_Price": {"$avg":"$ad_data.Price"},
             "Bikes": {"$sum" : 1 }}}]')



# Question 4 
m2$aggregate('[{
            "$match" : 
            {"ad_data.Price" : 
             { "$exists" : true }}}, 
        {"$group":{"_id": null,
        "Min_Price":{"$min":"$ad_data.Price"},
        "Max_Price":{"$max":"$ad_data.Price"}
      }}]')

# Question 5

#search for the word Negotiable and count the results
#we use the regex function for pattern matching strings and i as option for case insensitivity to match upper and lower cases
m2$aggregate('[{
      "$match" : 
		   {"metadata.model": 
		    {"$regex" : "Negotiable", "$options" : "i"} }}, 
      {"$group":{"_id": null,
		  "Bikes": {"$sum" : 1 }
		}}]')
