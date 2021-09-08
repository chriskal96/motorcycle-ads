library("redux")
library("dplyr")
library("tidyverse")


r <- redux::hiredis(
  redux::redis_config(
    host = "127.0.0.1", 
    port = "6379"))

modified_listings <- read.csv("modified_listings.csv", header = TRUE, sep=",", stringsAsFactors = FALSE)
emails_sent <- read.csv("emails_sent.csv", header = TRUE, sep=",", stringsAsFactors = FALSE)

r$FLUSHALL() # need to clean everything inside redis

# Question 1

#we use UserÉd as indicator in order to deal with users that exists more than once
for(i in 1:nrow(modified_listings))
{
  if ((modified_listings$ModifiedListing[i] ==1) & (modified_listings$MonthID[i]==1)) 
  {
    r$SETBIT("ModificationsJanuary",modified_listings$UserID[i],"1")
    
  }
}

r$BITCOUNT("ModificationsJanuary")

# Question 2
# using BITOP NOT we can find the users that have 0 in the bitmap ModificationsJanuary which are the users that did not modified their listing on January
r$BITOP("NOT","NoModificationsJanuary","ModificationsJanuary")
r$BITCOUNT("NoModificationsJanuary")

# Question 3
# first we create a bitmap for every month that at least one email was sent to users
for(i in 1:nrow(emails_sent))
{
  if (emails_sent$MonthID[i]==1)
  {
    r$SETBIT("EmailsJanuary",emails_sent$UserID[i],"1")
  }
  else if(emails_sent$MonthID[i]==2)
  {
    r$SETBIT("EmailsFebruary",emails_sent$UserID[i],"1")
    
  }
  else if (emails_sent$MonthID[i]==3)
  {
    r$SETBIT("EmailsMarch",emails_sent$UserID[i],"1")
    
  }
}

#this new bitmap indicates the users that received at leeast one email in all 3 months
r$BITOP("AND","EmailsOnMonths",c("EmailsJanuary","EmailsFebruary" , "EmailsMarch"))

r$BITCOUNT("EmailsOnMonths")

#Question 4
#first a bitmap with the emails that users received on january and march
r$BITOP("AND", "January&March", c("EmailsJanuary", "EmailsMarch"))
#then a bitmap with the users that did not receive an email on february
r$BITOP("NOT", "NotFebruary", "EmailsFebruary")
#finaly the answer is a bitmap that came up combining the previous 2 with the bitop AND
r$BITOP("AND", "answer", c("January&March", "NotFebruary"))

r$BITCOUNT("answer")


#Question 5
#first we find the users that did not opened the email on January
for(i in 1:nrow(emails_sent)) {
  if ((emails_sent$MonthID[i]==1) & (emails_sent$EmailOpened[i]==0)){
    r$SETBIT("EmailsNotOpenedJanuary",emails_sent$UserID[i],"1")
  }
}

r$BITCOUNT("EmailsNotOpenedJanuary")

#using the ModificationsJanuary from question 1 and combine it with the new EmailsNotOpenedJanuary , we get the users that did 
#not opened the email but modfied their listing
r$BITOP("AND","EmailsNotOpenedJanuaryButModified",c("ModificationsJanuary","EmailsNotOpenedJanuary"))

r$BITCOUNT("EmailsNotOpenedJanuaryButModified")

#Question 6
#find the users that did not opened the email on February
for(i in 1:nrow(emails_sent)) {
  if ((emails_sent$MonthID[i]==2) & (emails_sent$EmailOpened[i]==0)){
    r$SETBIT("EmailsNotOpenedFebruary",emails_sent$UserID[i],"1")
  }
}
#find the users that did modified their listing on February
for(i in 1:nrow(modified_listings))
{
  if ((modified_listings$ModifiedListing[i] ==1) & (modified_listings$MonthID[i]==2)) 
  {
    r$SETBIT("ModificationsFebruary",modified_listings$UserID[i],"1")
  }
}

#find the users that did not opened the email on March
for(i in 1:nrow(emails_sent)) {
  if ((emails_sent$MonthID[i]==3) & (emails_sent$EmailOpened[i]==0)){
    r$SETBIT("EmailsNotOpenedMarch",emails_sent$UserID[i],"1")
  }
}

#find the users that did modified their listing on March
for(i in 1:nrow(modified_listings))
{
  if ((modified_listings$ModifiedListing[i] ==1) & (modified_listings$MonthID[i]==3)) 
  {
    r$SETBIT("ModificationsMarch",modified_listings$UserID[i],"1")
  }
}

# find the users that did not opened the email on February but modified their listing
r$BITOP("AND","EmailsNotOpenedFebruaryButModified",c("ModificationsFebruary","EmailsNotOpenedFebruary"))
# find the users that did not opened the email on March but modified their listing
r$BITOP("AND","EmailsNotOpenedMarchButModified",c("ModificationsMarch","EmailsNotOpenedMarch"))

r$BITCOUNT("EmailsNotOpenedJanuaryButModified")
r$BITCOUNT("EmailsNotOpenedFebruaryButModified")
r$BITCOUNT("EmailsNotOpenedMarchButModified")

# find the users that did not opened the email but modified their listing on January or February or March using the bitop OR
r$BITOP("OR","EmailsNotOpenedButModified",c("EmailsNotOpenedJanuaryButModified","EmailsNotOpenedFebruaryButModified","EmailsNotOpenedMarchButModified"))

r$BITCOUNT("EmailsNotOpenedButModified")

#Question 7

# we join the 2 datasets in order to select users that have criteria that apply in those 2 datasets simultaneously
merged_dataset<-merge(x=emails_sent, y=modified_listings, by=c("UserID","MonthID"))

# E-mails opened per month with the listing to be modified
for(i in 1:nrow(merged_dataset))
{
  if ((merged_dataset$EmailOpened[i]==1) & (merged_dataset$ModifiedListing[i] ==1)) {
    if (merged_dataset$MonthID[i]==1) {
      r$SETBIT("EmailsOpenedAndModifiedJanuary",merged_dataset$UserID[i],"1")
    } else if (merged_dataset$MonthID[i]==2) {
      r$SETBIT("EmailsOpenedAndModifiedFebruary",merged_dataset$UserID[i],"1")
    } else {
      r$SETBIT("EmailsOpenedAndModifiedMarch",merged_dataset$UserID[i],"1")
    }
  }
}

# E-mails opened per month without the listing to be modified
for(i in 1:nrow(merged_dataset))
{
  if ((merged_dataset$EmailOpened[i]==1) & (merged_dataset$ModifiedListing[i] ==0)) {
    if (merged_dataset$MonthID[i]==1) {
      r$SETBIT("EmailsOpenedNotModifiedJanuary",merged_dataset$UserID[i],"1")
    } else if (merged_dataset$MonthID[i]==2) {
      r$SETBIT("EmailsOpenedNotModifiedFebruary",merged_dataset$UserID[i],"1")
    } else {
      r$SETBIT("EmailsOpenedNotModifiedMarch",merged_dataset$UserID[i],"1")
    }
  }
}

# percentage of users that opened and modified their listing on January
January_open_modify <- r$BITCOUNT("EmailsOpenedAndModifiedJanuary") /(r$BITCOUNT("EmailsOpenedAndModifiedJanuary")+r$BITCOUNT("EmailsOpenedNotModifiedJanuary")) * 100
January_open_modify

# percentage of users that opened and modified their listing on February
February_open_modify <- (r$BITCOUNT("EmailsOpenedAndModifiedFebruary") / (r$BITCOUNT("EmailsOpenedAndModifiedFebruary") + r$BITCOUNT("EmailsOpenedNotModifiedFebruary"))) * 100
February_open_modify

# percentage of users that opened and modified their listing on March
March_open_modify <- (r$BITCOUNT("EmailsOpenedAndModifiedMarch") / (r$BITCOUNT("EmailsOpenedAndModifiedMarch") + r$BITCOUNT("EmailsOpenedNotModifiedMarch"))) * 100
March_open_modify


