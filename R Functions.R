#source('C:/Users/SOL_LENOVO_1/Documents/R functions.txt')
library(data.table)
library(dplyr)
library(lubridate)
library(stringr)
library(qdapRegex)
library(lpSolveAPI)
library(matrixStats)
library(caret)

#==================================================
#  Read All file in the Folder (txt,csv.....etc)
#==================================================
#txt

Read_Folder_txt = function(Path)
{
  setwd(Path)
  
  files = data.frame(name = list.files(pattern='*.txt'))
  files$name = paste0(Path,files$name)
  files = rbindlist(lapply(files$name, fread, header=FALSE))
  return(files)
}

#csv

Read_Folder_csv = function(Path)
{
  setwd(Path)
  
  files = data.frame(name = list.files(pattern='*.csv'))
  files$name = paste0(Path,files$name)
  files = rbindlist(lapply(files$name, fread, header=FALSE))
  return(files)
}


#=============================================================#
#                   Split the file by Concepts                #
#=============================================================#

Get_Filename_ByConcepts = function(df)
{
  df = df %>%
    select(customer_Number, Concept1, Concept2, Concept3, Concept4) %>%
    mutate(BS = ifelse(Concept1 > 0,'C1',''),
           LS = ifelse(Concept2 > 0,'C2',''),
           SM = ifelse(Concept3 > 0,'C3',''),
           SP = ifelse(Concept4 > 0,'C4',''))  %>%
    mutate(Segment = str_replace_all(rm_white(paste(C1, C2, C3, C4))," ","_")) %>%
    data.table()
  
  df$Segment[df$Segment==''] = 'C1_C2_C3_C4'
  
  df = select(df, customer_Number, Segment)
  
  return(df)
}


#=============================================================#
# Check for duplicate customer numbers                        #
#=============================================================#

dups= function(df)
{ 
  print(paste0("Total : ", length(df$customer_Number), "   Unique : ", length(unique(df$customer_Number))))
}


#=============================================================#
#   Coping data Excel to R 
#=============================================================#

read.excel <- function(header=TRUE) 
{
  read.table("clipboard",sep="\t",header=header)
}


#=============================================================#
# Copy the output for pasting into excel                      #
#=============================================================#

copy = function(df)
{
  write.table(df, "clipboard", sep="\t", row.names = FALSE)
}

#=============================================================#
# write the file in Txt format                                #
#=============================================================#

txt = function(df,Path,HLQ,file)
{
  fwrite(df[,'customer_Number'],paste0(Path,HLQ,file,'.txt'),col.names=FALSE)
}

#=============================================================#
# table in new arranged format                                #
#=============================================================#

newtable = function(Base,Sampling,Customer_Group)
{
  with(Base, table(Sampling, Customer_Group))[c(3,2,1),c(1,3,2,4)]
}

#=============================================================#
# write the file in csv format  (csv)                              #
#=============================================================#

csv = function(df,Path,HLQ)
{
  fwrite(df[,1],paste0(Path,HLQ,'.csv'),col.names=FALSE)
}

#=============================================================#
# Calculate the R,F,M and RFM Score                           #
#=============================================================#

RFM = function(df,R=1,F=1,M=1)
{
  df$MaxTransactionDate = ymd(df$MaxTransactionDate)
  
  df$Recency = ntile(df$MaxTransactionDate,4)-1  
  df$Frequency = ntile(df$BillCount,4)-1
  df$Monetary = ntile(df$TotalAmount,4)-1
  df$RFM_Score = ((R*df$Recency + F*df$Frequency + M*df$Monetary)*10) 
  
  return(df)
}

#=============================================================#
# Display the RFM grid                                        #
#=============================================================#

RFM_Summary = function(df)
{
  RFM_DT = as.data.table(df)
  
  RFM_DT = RFM_DT[,.(RFM_Score = mean(RFM_Score),
                     Customers=.N,
                     Min_Amount = min(TotalAmount),
                     Max_Amount = max(TotalAmount),
                     Min_BillCount = min(BillCount),
                     Max_Billcount = max(BillCount),
                     Min_Date = min(MaxTransactionDate),
                     Max_Date=max(MaxTransactionDate)), 
                  
                  by=c("Recency","Frequency", "Monetary")]
  
  RFM_DT = setorder(RFM_DT, -RFM_Score, -Recency, -Frequency, -Monetary)
  return(RFM_DT)
}


#==================================================================#
# Select the sample based on the required count From Groups       #
#==================================================================#

Select_Sample = function(basefile, Cust_Group, n1, n2, n3, n4,seed)
{
  set.seed(seed)
  
  basefile = basefile %>% select(customer_Number, Customer_Group, Sampling, RevenueBand, Revenue) %>%
    filter(Customer_Group == Cust_Group & Sampling == "RG")
  
  df1 = basefile %>% filter(RevenueBand == "BELOW25") 
  if (nrow(df1) < n1) n1 = nrow(df1)
  df1 = df1 %>% sample_n(n1)
  
  df2 = basefile %>% filter(RevenueBand == "25-50")
  if (nrow(df2) < n2) n2 = nrow(df2)
  df2 = df2 %>% sample_n(n2)
  
  df3 = basefile %>% filter(RevenueBand == "50-75")
  if (nrow(df3) < n3) n3 = nrow(df3)
  df3 = df3 %>% sample_n(n3)
  
  df4 = basefile %>% filter(RevenueBand == "ABOVE75")
  if (nrow(df4) < n4) n4 = nrow(df4)
  df4 = df4 %>% sample_n(n4)
  
  df = data.table(rbind(df1, df2, df3, df4))
  return(df)
  
}


#=============================================================#
# Verify Upload filenames and counts and check for duplicates #
#=============================================================#

Uploads_Summary = function(Upload_Dir)
{
  Path = Upload_Dir
  setwd(Path)
  
  files = data.frame(name=list.files(pattern="*.txt"))
  files$path = paste0(Path,"/",files$name)
  files$size = file.size(files$path)
  
  # Remove any empty files as that throws an error while appending later
  files = files[files$size > 0,]
  
  # Get the record counts
  for (i in 1:length(files$path))
  {
    t = fread(files$path[i])
    files$Records[i] = nrow(t)
  }
  
  # Read and all the files into lists and then append them
  data = lapply(files$path,fread, header=FALSE)
  dt = rbindlist(data)
  
  files$path = NULL
  files$size = NULL
  files$name = as.character(files$name)
  files$name_length = str_length(files$name)
  
  print(paste("Total Records :", nrow(dt), "Unique Records :",nrow(unique(dt))))
  
  # Check for any spaces in the file name
  check =  filter(files,grepl(' ',name))
  if(nrow(check)==0) 
  {
    print('No spaces found in filenames')
  } else 
  {
    print('The following file names contain spaces:')
    print(check$name)
  }
  
  
  # Check for length of filename
  check =  filter(files,name_length>44)
  if(nrow(check)==0) 
  {
    print('All file name lengths are <= 40 characters')
  } else 
  {
    print('The following file names exceed 40 characters:')
    print(check[,c('name', 'name_length')])
  }
  
  View(files[,c('name','Records')]) 
  
}



#=============================================================#
# Create modified Base file to get similar TG and RG AMS      #
#=============================================================#

Create_TopN_BaseFile = function(Basefile, Group, N1, Seg1="H", N2, Seg2="H",N3, Seg3="H", N4, Seg4="H")
{
  
  set.seed(100)
  
  Base_ActiveRG = Basefile %>% 
    filter(Customer_Group == Group & Sampling == "RG") %>% 
    arrange(-Revenue) %>%
    data.table()
  
  df1 = Base_ActiveRG %>% filter(RevenueBand == "BELOW25")
  if (Seg1=="H") df1 = head(df1,N1) else df1 = tail(df1,N1)
  
  df2 = Base_ActiveRG %>% filter(RevenueBand == "25-50")
  if (Seg2=="H") df2 = head(df2,N2) else df2 = tail(df2,N2)
  
  df3 = Base_ActiveRG %>% filter(RevenueBand == "50-75")
  if (Seg3=="H") df3 = head(df3,N3) else df3 = tail(df3,N3)
  
  df4 = Base_ActiveRG %>% filter(RevenueBand == "ABOVE75")
  if (Seg4=="H") df4 = head(df4,N4) else df4 = tail(df4,N4)
  
  rm(Base_ActiveRG)
  
  df = setDT(bind_rows(df1,df2,df3,df4))
  return(df)
}



#=============================================================#
# Create filename based on Customer_Group and Gender + Age    #
#=============================================================#

Get_Filename_ByGender = function(df)
{
  
  df = df %>%
    rename("NB"="NewBornAmount",
           "InfB"= "InfantBoysAmount",
           "InfG"="InfantGirlsAmount",
           "InfO"="InfantOtherAmount",
           "ToddB"="ToddlerBoysAmount",
           "ToddG"="ToddlerGirlsAmount",
           "ToddO"="ToddlerOthersAmount",
           "teenB"="TeenBoysAmount",
           "teenG"="TeenGirlsAmount",
           "teenO"="TeenOthersAmount") %>%
    mutate(Group = str_replace(str_to_title(df$Customer_Group)," ","_")) %>%
    select(customer_Number, 
           Group, 
           NB,
           InfB,
           InfG,
           InfO,
           ToddB,
           ToddG,
           ToddO,
           teenB,    
           teenG,   
           teenO)
  
  
  df$Segment = colnames(df[,c(3:12)])[max.col(df[,c(3:12)])]
  
  df$Filename = paste(df$Segment, sep="_")
  
  df = df %>% select(customer_Number, Filename)
  
  setDT(df)
  
  return(df)
}

#=============================================================#
# Get the count and AMS breakup by Revenue Bands              #
#=============================================================#

Revenue_Summary = function(df)
{
  df = setDT(df)
  
  table1 = df[,.(count=length(customer_Number), AMS = round(mean(Revenue),1)),by="RevenueBand"] 
  setorder(table1,RevenueBand)
  
  table2 = df[,.(count=length(customer_Number), AMS = round(mean(Revenue),1))] 
  
  table3 = bind_rows(table1, table2)[c(4,1,2,3,5),]
  table3[nrow(table3),1] = ""
  View(table3)
  
  
}


#=========================================================================#
#         Core and Loyal Analysis functions   (Core , Loyal , Overall )   #
#=========================================================================#

Core=function(Data)
{
  Core=Data %>% filter(billCount >=3) %>% data.table()
  Core_Summary=Core[,.(Count=.N,Revenue=sum(TotalAmount))]
  
  return(Core_Summary)
}

Loyal=function(Data)
{
  loyal=Data %>% filter(billCount >=2) %>% data.table()
  Loyal_Summary=loyal[,.(Count=.N,Revenue=sum(TotalAmount))]
  return(Loyal_Summary)
}

Overall=function(Data)
{
  summ=Data[,.(Count=.N , Revenue=sum(TotalAmount))]
  return(summ)  
}



#-------------------------------------------
# Age Group splitting
#-------------------------------------------
  
Get_Age_Group_Mix = function(File, num_recs, NewBorn, Teen, Infant,Toddler)
{
  set.seed(200)
  
  NewBorn = round(num_recs * NewBorn / 100, 0)
  Teen = round(num_recs * Teen / 100, 0)
  Infant = round(num_recs * Infant / 100, 0)
  Toddler = round(num_recs * Toddler/ 100, 0)
  
  DF1 = File %>%
    filter(Age_Group == 'NewBornAmount') %>%
    arrange(desc(RFM_Score)) %>%
    head(NewBorn) %>%
    data.table()
  
  DF2 = File %>%
    filter(Age_Group == 'TeenAmount') %>%
    arrange(desc(RFM_Score)) %>%
    head(Teen) %>%
    data.table()
  
  DF3 = File %>%
    filter(Age_Group == 'InfantAmount') %>%
    arrange(desc(RFM_Score)) %>%
    head(Infant) %>%
    data.table()
  
  DF4 = File %>%
    filter(Age_Group == 'ToddlerAmount') %>%
    arrange(desc(RFM_Score)) %>%
    head(Toddler) %>%
    data.table()
  
  Final = bind_rows(DF1, DF2, DF3, DF4)
  return(Final)
}
