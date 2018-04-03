# Code for AIS BAU Daily Run RFC
# Author - PwC/Liquan
# Last Modified on 20-03-2018



times <- 3
while(times>0){
  e <- tryCatch(
    withCallingHandlers({
      
while(TRUE) {
  
  
  
  options(java.parameters = "- Xmx1024m")
  
  if(('RJDBC' %in% rownames(installed.packages())) == FALSE) {install.packages('RJDBC')}
  if(('aws.s3' %in% rownames(installed.packages())) == FALSE) {install.packages('aws.s3')}
  if(('RODBC' %in% rownames(installed.packages())) == FALSE) {install.packages('RODBC')}
  if(('sqldf' %in% rownames(installed.packages())) == FALSE) {install.packages('sqldf')}
  if(('dplyr' %in% rownames(installed.packages())) == FALSE) {install.packages('dplyr')}
  if(('devtools' %in% rownames(installed.packages())) == FALSE) {install.packages('devtools')}
  if(('data.table' %in% rownames(installed.packages())) == FALSE) {install.packages('data.table')}
  if(('xts' %in% rownames(installed.packages())) == FALSE) {install.packages('xts')}
  if(('zoo' %in% rownames(installed.packages())) == FALSE) {install.packages('zoo')}
  if(('XLConnect' %in% rownames(installed.packages())) == FALSE) {install.packages('XLConnect')}
  
  library(RJDBC)
  library(aws.s3)
  library(RODBC)
  library(sqldf)
  library(dplyr)
  require(devtools)
  library(data.table)
  library(xts)
  library(zoo)
  library(XLConnect)
  
  
  xlcFreeMemory()
  rm(list=ls())
  
  parameters <- read.csv("D:/R_SCRIPTS/Liquan/Prod/parameters.csv",stringsAsFactors = FALSE)
  
  sink("D:/R_SCRIPTS/Liquan/Prod/Logfiles/BOBJ_AIS_Daily_log.txt",append = TRUE)
  
  # *************************************************************************************************
  # *************************************************************************************************
  # *************************************************************************************************
  # *************************************************************************************************
  # *************************************************************************************************
  #These inputs need to be changed if the credentials or the ODBC driver changes
  input_odbc_driver = parameters$input_odbc_driver #"Freight Analytics - PROD" #"Freight Analytics - UAT"
  input_username = parameters$input_username
  input_password = parameters$input_password
  input_aws_access_key_id = parameters$input_aws_access_key_id
  input_aws_secret_access_key = parameters$input_aws_secret_access_key
  input_bucket = parameters$input_bucket
  input_schema = parameters$input_schema
  
  # this is where the extracts - VL, FSR, VSR, S5 and FAR reports will be found
  input_s3_bucket_to_read_raw_files = parameters$input_s3_bucket_to_read_raw_files
  
  # this is where temp and intermediate files get stored and retrieved
  input_s3_bucket_to_store_data = parameters$input_s3_bucket_to_store_data
  input_s3_bucket_short_path = parameters$input_s3_bucket_short_path
  
  # the users to grant permissions for
  input_users = parameters$input_users 
  
  today_date = toString(Sys.Date())
  today_date = '2018-04-02' #to run for a certain date, change it here
  today_date

  
  #Input ports to filter Vessel List Data
  input_vl_allowed_ports = c('168','144','136','126','94','460','439','450','456')
  
  # ***********************Input Details for data import from XLSX files******************************
  
  # Input the delimiter for text files
  input_delimiter = '|'
  
  # Number of lines to skip in the file
  input_num_lines_to_skip_FSR = 0
  input_num_lines_to_skip_VSR = 0
  input_num_lines_to_skip_VL = 0
  input_num_lines_to_skip_FAR = 0
  
  
  # **********************************Input List of Cols*********************************************
  
  input_fsr_list_cols <- c("Commodity",
                           "MatGrp",
                           "MatGrpName",
                           "Mat",
                           "MatName",
                           "NomItmChDat",
                           "NomTechKey",
                           "NomNum",
                           "NomItm",
                           "FrgtVoyNum",
                           "FrgtVoyItmNumb",
                           "Route",
                           "RouteName",
                           "DepCtry.Route.",
                           "DepCtry.Route.Name",
                           "DestPoint.Route.",
                           "DestPoint.Route.Name",
                           "DisPort",
                           "VesExtIdent",
                           "VehNum",
                           "LloydsNum",
                           "IncoTerms1",
                           "CompCode",
                           "CompName",
                           "Plant.Loc.",
                           "Plant.Loc.Name",
                           "ShipToParty",
                           "ShipToPartyName",
                           "SoldToParty",
                           "SoldToPartyName",
                           "BOLDat",
                           "ETADat",
                           "ETADDat",
                           "ETAODat",
                           "ETAVDat",
                           "ETBDat",
                           "ETBDDate",
                           "ETCDDat",
                           "ETDDat",
                           "ETDDDat",
                           "FASTDat",
                           "NORADat",
                           "SchdDate",
                           "SchledQty",
                           "ActPstedQty")
  
  input_vsr_list_cols <- c("Voyage.Number",
                           "NominationKey",
                           "NomKeyItem",
                           "LoadPort",
                           "Disport",
                           "Shipper",
                           "VoyOperator",
                           "Trader",
                           "VoyClsdStat",
                           "Material",
                           "ArrDateVer",
                           "ArrvlDate",
                           "VoyB.LDate",
                           "DepDateVer",
                           "DepDate",
                           "VoyTECOStat",
                           "FirstVoyTECODate",
                           "VoyTECODate",
                           "VoyType",
                           "FreightDesk",
                           "Contract",
                           "CustID",
                           "CustName",
                           "Vessel",
                           "DeadWeight",
                           "WeiofEmptyVehicle",
                           "MaxWeiVeh",
                           "VehLen",
                           "MaxVehDraf",
                           "EquipNo",
                           "Equip_No_Name",
                           "Carrier.Vendor.",
                           "VehBuildYear",
                           "LloydsNum",
                           "TD.DimUoM",
                           "MaxVolforVeh",
                           "VehHeight",
                           "TransPlanPoint",
                           "VehWidth",
                           "TPC",
                           "DraftWeightUOM",
                           "VehRoute",
                           "TD.VolUnit",
                           "TD.WeightUnit",
                           "TD.VehType",
                           "ContrActQty")
  
  
  
  input_vl_list_cols <- c("DWT.Group",
                          "Vessel.Name",
                          "Vessel.Number",
                          "Vessel.DWT",
                          "Vessel.Length",
                          "Vessel.ETA",
                          "Previous.ETA.at.Next.LP",
                          "ETA.at.Next.LP.Change..Days.",
                          "Laycan.From",
                          "Laycan.To",
                          "Document.Type",
                          "Demurrage.Amount",
                          "Hire.Rate",
                          "eBills",
                          "Voyage.Overall.Status",
                          "New.Fix",
                          "Current.Voyage.Number",
                          "Nomination.Item.Comments",
                          "Voyage.Comments",
                          "Contract.Number..Buy..Description",
                          "Customer.Name",
                          "Vessel.vetting.date",
                          "Vessel.Vetting.ID",
                          "Current.ETA.at.Next.LP",
                          "Arrival.Date",
                          "Hire.Indicator",
                          "Hire.Rate.Currency",
                          "Customer.Number",
                          "Demurrage.Currency",
                          "Previous.Voyage.Number",
                          "Voyage.Created.On",
                          "COA.Item",
                          "Contract.Number..Buy.",
                          "Voyage.Operator",
                          "Buy.Contract.Company.Code",
                          "Open.Lifts",
                          "Vendor",
                          "Buy.Contract.Comments",
                          "REDEL.Date",
                          "Vendor.Name",
                          "Contract.Number..Sell.",
                          "Contract.Number..Sell..Description",
                          "Sell.Contract.Comments",
                          "Handed.over.Nominations", 
                          "Nomination..technical..Key",
                          "Nomination.Key.Item",
                          "ETA.Date..Nomination.",
                          "Port",
                          "Port.Name",
                          "Voyage.Operator.Name")
  
  
  input_s5_iron_ore_list_cols <- c("S5.Ref..",
                                   "Operator",
                                   "Voy...",
                                   "Vessel",
                                   "Port",
                                   "ETA",
                                   "ETB",
                                   "ETD",
                                   "Remark",
                                   "LOI.accepted",
                                   "Risk.of.delay.to.berth",
                                   "Reason",
                                   "Variance.T.1",
                                   "ETA.P.Hedland",
                                   "Receiver")
  
  input_s5_coal_list_cols <- c("S5.Ref..",
                               "Operator",
                               "Voy...",
                               "Vessel",
                               "Port",
                               "ETA",
                               "ETB",
                               "ETD",
                               "Remark",
                               "LOI.accepted",
                               "Risk.of.delay.to.berth",
                               "Reason",
                               "Variance.T.1",
                               "ETA.Next.Port",
                               "Receiver")
  
  input_far_list_cols <- c("TMU",
                           "Commodity",
                           "NominationTechnicalKey",
                           "NominationItem",
                           "NominationNumber",
                           "SchdDateforNom",
                           "LCLPDate",
                           "BOLDate",
                           "CompanyCode",
                           "CompanyName",
                           "VesselCode",
                           "Carrier_Vendor",
                           "FreightVoyage",
                           "VoyageType",
                           "VoyageDurationStart",
                           "VoyageDurationEnd",
                           "TCOperator",
                           "Material",
                           "MaterialName",
                           "LoadPort",
                           "DischargePort",
                           "Discharge_Port_Description",
                           "CTContractYear",
                           "CTContractNumber",
                           "CTContractDate",
                           "Business_Partner",
                           "Business_Partner_desc",
                           "CTDeliveryLeg",
                           "Curve",
                           "ActualQuantity")
  
  
  # Ensures that large numbers are not shortened to exponential form
  options("scipen"=30)
  
  
  # Gets the folder with the daily extracts
  Sys.setenv("AWS_ACCESS_KEY_ID" = input_aws_access_key_id,"AWS_SECRET_ACCESS_KEY" = input_aws_secret_access_key)
  bucket <- get_bucket(bucket = input_bucket, key = input_aws_access_key_id, secret=input_aws_secret_access_key,prefix=input_s3_bucket_to_read_raw_files)
  
  
  
  today_date_formatted = as.Date(today_date,format = "%Y-%m-%d")
  
  previous_date = today_date_formatted - 1
  
  previous_date_formatted = as.Date(previous_date,format = "%Y-%m-%d")
  
  
  date_stripped = gsub("-","",today_date)
  
  #********************************** Filename: 1. AIS Future State Daily Data 20171016.R
  # print("1. AIS Future State Daily Data 20171016.R")
  print(paste0("1. AIS Future State Daily Data ",today_date,".R"))
  
  while (TRUE){
    
    
    # Description: 
    #  - This R code gets the daily extracts of Vessel List, FSR and VSR. (automatically finds the latest file)
    #  - If the latest file was not updated in the current date, then the code will drop down the previous days data for those vessels that were tagged the previous day
    #  - Formats and adds the necessary columns
    #  - Appends the final data to their respective existing tables
    
    # Dependencies: 
    #   1: The file names must be in the following formats for the respective files for this code to work
    #       a) FSR_extracted yyyymmdd.xlsx
    #       a) VSR_extracted yyyymmdd.xlsx
    #       a) Vsl List _extracted yyyymmdd.xlsx
    #   2: The daily extracts must be in the S3 bucket with the following file path: 'BHP_PwC/7. Daily extracts'
    #       - If not please go to *inputs* section at the top and change the path
    #   3: Username used to connect to redshift is dapappadmin (hence this user must be given access in order for this to run.)
    # 
    # Assumptions: NA
    
    
    #################################### Section 1: Daily SAP Ingestion
    
    setwd('D:/R_SCRIPTS/Liquan/Prod/Dailyfiles/')
    
    # Start value
    # FSR_file_name = paste0(input_s3_bucket_to_read_raw_files,"FSR Daily - BOBJ_20180401.xlsx")
    # VSR_file_name = paste0(input_s3_bucket_to_read_raw_files,"FVSR Daily - BOBJ_20180401.xlsx")
    # VL_file_name = paste0(input_s3_bucket_to_read_raw_files,"Vessel List - EQ1_20180401.XLSX")
    # 
    FSR_file_name = 'FSR Daily - BOBJ_20180401.xlsx'
    VSR_file_name = 'FVSR Daily - BOBJ_20180401.xlsx'
    VL_file_name = 'Vessel List - EQ1_20180401.XLSX'
    
    # Loop through all files for FSR, VSR and VL to find the latest file for each
    # for (file in bucket){
    #   
    #   #FSR
    #   if(substr(file$Key,35,41) == 'MKT_FSR') {
    #     FSR_Latest_File = substr(file$Key,47,54)
    #     if(FSR_Latest_File == date_stripped){
    #       FSR_file_name = file$Key
    #     }
    #   }
    #   
    #   #vSR
    #   if(substr(file$Key,35,42) == 'MKT_FVSR'){
    #     VSR_Latest_File = substr(file$Key,50,57)
    #     if(VSR_Latest_File == date_stripped){
    #       VSR_file_name = file$Key
    #     }
    #   }
    #   
    #   #VL
    #   if(substr(file$Key,35,48) == 'MKT_VesselList'){ 
    #     VL_Latest_File = substr(file$Key,50,57)
    #     if(VL_Latest_File == date_stripped){
    #       VL_file_name = file$Key
    #     } 
    #   }
    #   
    # }
    
    print(FSR_file_name)
    print(VSR_file_name)
    print(VL_file_name)
    
    
    # ******************************** FSR Daily *****************************
    # This section gets the FSR extracts from the S3 bucket, cleans the data and uploads to redshift
    
    # Get the date from file name
    # get_date <- substr(FSR_file_name,47,54)
    # get_date
    # 
    # date_formatted <- paste0(substring(get_date,1,4),'-',substring(get_date,5,6),'-',substring(get_date,7,8))
    # date_formatted
    
    date_formatted <- substr(FSR_file_name,18,25)
    
    if (today_date == date_formatted) {
      print("We have the latest date data")
      
      
      # Getting the FSR daily extract from the S3 bucket
      workbook <- XLConnect::loadWorkbook(FSR_file_name)
      fsr_data <- readWorksheet(workbook, sheet='FSR(Nominations)', header=TRUE,startRow = 8,startCol = 2)
      colnames(fsr_data) <- input_fsr_list_cols
      
      
      
      
      # Removing duplicates, and empty rows
      fsr_new <- unique(fsr_data)
      
      # Removing rows with lloyds number = 0 or null
      fsr_new <- fsr_new[!(is.na(fsr_new$LloydsNum) | fsr_new$LloydsNum=="" | fsr_new$LloydsNum=="0" | fsr_new$LloydsNum=="#"), ]
      
      
      fsr_new$NomItmChDat = as.Date(fsr_new$NomItmChDat,"%d.%m.%Y")
      fsr_new$BOLDat = as.Date(fsr_new$BOLDat,"%d.%m.%Y")
      fsr_new$ETADat = as.Date(fsr_new$ETADat,"%d.%m.%Y")
      fsr_new$ETADDat = as.Date(fsr_new$ETADDat,"%d.%m.%Y")
      fsr_new$ETAODat = as.Date(fsr_new$ETAODat,"%d.%m.%Y")
      fsr_new$ETAVDat = as.Date(fsr_new$ETAVDat,"%d.%m.%Y")
      fsr_new$ETBDat = as.Date(fsr_new$ETBDat,"%d.%m.%Y")
      fsr_new$ETBDDate = as.Date(fsr_new$ETBDDate,"%d.%m.%Y")
      fsr_new$ETCDDat = as.Date(fsr_new$ETCDDat,"%d.%m.%Y")
      fsr_new$ETDDat = as.Date(fsr_new$ETDDat,"%d.%m.%Y")
      fsr_new$ETDDDat = as.Date(fsr_new$ETDDDat,"%d.%m.%Y")
      fsr_new$FASTDat = as.Date(fsr_new$FASTDat,"%d.%m.%Y")
      fsr_new$NORADat = as.Date(fsr_new$NORADat,"%d.%m.%Y")
      fsr_new$SchdDate = as.Date(fsr_new$SchdDate,"%d.%m.%Y")
      
      
      fsr_new$merge_key <- paste(fsr_new$NomNum,fsr_new$NomItm)
      
      
      fsr_new <- fsr_new %>% group_by(merge_key) %>% filter(NomItmChDat == max(NomItmChDat))
      
      fsr_new <- fsr_new[,input_fsr_list_cols]
      
      
      # Adding Selected Date to the data
      fsr_new$SelectedDate = as.Date(date_formatted,format = "%Y-%m-%d")
      
      
      # Setting a flag to indicate that the data is not copied
      fsr_new$FSR_Dropdown_Flag = '0'
      fsr_new$FSR_last_Updated_On_Date = as.Date(date_formatted,format = "%Y-%m-%d")
      
      #fsr_new <- as.data.frame(apply(fsr_new,2,function(x)gsub('NA-NA-NA NA', '',x)))
      #fsr_new <- as.data.frame(apply(fsr_new,2,function(x)gsub('NA-NA-NA', '',x)))
      
      # write to temp file
      tempFileStorage <- rawConnection(raw(0),"r+")
      
      write.table(fsr_new,tempFileStorage,sep=",",row.names = F, col.names=F)
      
      filename = paste0("fsr_daily_dump_bobj.csv")
      filename
      
      # Upload to S3
      put_object(file = rawConnectionValue(tempFileStorage), object = paste0(input_s3_bucket_to_store_data,filename), bucket = input_bucket)
      
      # close temporary connection
      close(tempFileStorage)
      
      # Channel is opened
      channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
      
      
      src_data_name <- filename
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("COPY ",input_schema,".fsr_master_daily_data
                                        FROM '",input_s3_bucket_to_store_data,filename,"'
                                        access_key_id '",input_aws_access_key_id,"' 
                                        secret_access_key '",input_aws_secret_access_key,"' 
                                        NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('fsr_master_daily_data', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU and SAFETY_BAU','", src_data_name,"','", end_ts,"')"))
      
      # Append the newly created FSR data
      #       sqlQuery(channel, paste0("COPY ",input_schema,".fsr_master_daily_data 
      #                                FROM '",input_s3_bucket_to_store_data,filename,"'
      #                                access_key_id '",input_aws_access_key_id,"' 
      #                                secret_access_key '",input_aws_secret_access_key,"' 
      #                                NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"))
      
      # vacuum the table
      sqlQuery(channel, paste0("VACUUM ",input_schema,".fsr_master_daily_data;"))
      
      src_data_name <- 'AIS_VL_FSR_VSR_Daily'
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      fsr_vl_imo = sqlQuery(channel, paste0("WITH temp_master_vessel_list AS
                                            (SELECT DISTINCT ais_vl_fsr_vsr_vessel_name, ais_vl_fsr_vsr_imo, TRIM(fsr_vehicle_number) as fsr_vehicle_number, ais_vl_fsr_vsr_date_position,
                                            REGEXP_REPLACE(ais_vl_fsr_vsr_vessel_name, '[^a-zA-Z0-9]+', '') as vessel_name_cleaned,
                                            DENSE_RANK() OVER (PARTITION BY ais_vl_fsr_vsr_imo, fsr_vehicle_number
                                            ORDER BY ais_vl_fsr_vsr_date_position DESC) as rank
                                            FROM ",input_schema,".AIS_VL_FSR_VSR_Daily
                                            WHERE ais_vl_fsr_vsr_date_position = '",previous_date_formatted,"'
                                            AND ais_static_imo IS NOT NULL
                                            AND fsr_vehicle_number IS NOT NULL)
                                            SELECT *
                                            FROM temp_master_vessel_list
                                            WHERE rank = 1
                                            ORDER BY ais_vl_fsr_vsr_imo, fsr_vehicle_number, ais_vl_fsr_vsr_date_position;"), FALSE)
      
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- fsr_vl_imo
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('temp_master_vessel_list', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      
      # Close the channel
      odbcClose(channel)
      
      
      # write to temp file
      tempFileStorage <- rawConnection(raw(0),"r+")
      
      write.table(fsr_vl_imo,tempFileStorage,sep=",",row.names = F, col.names=F)
      
      filename_vl_imo = paste0("temp_vessel_list_imo.csv")
      
      # Upload to S3
      put_object(file = rawConnectionValue(tempFileStorage), object = paste0(input_s3_bucket_to_store_data,filename_vl_imo), bucket = input_bucket)
      
      # close temporary connection
      close(tempFileStorage)
      
      # Channel is opened
      channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
      
      
      
      
      
      
      
      
      src_data_name <- filename_vl_imo
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("COPY ",input_schema,".vessel_list_imo_master
                                        FROM '",input_s3_bucket_to_store_data,filename_vl_imo,"'
                                        access_key_id '",input_aws_access_key_id,"'
                                        secret_access_key '",input_aws_secret_access_key,"'
                                        NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('vessel_list_imo_master', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      
      
      
      # Append the newly created data
      #       sqlQuery(channel, paste0("COPY ",input_schema,".vessel_list_imo_master
      #                                FROM '",input_s3_bucket_to_store_data,filename_vl_imo,"'
      #                                access_key_id '",input_aws_access_key_id,"'
      #                                secret_access_key '",input_aws_secret_access_key,"'
      #                                NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"))
      
      # Append to the list of imo and vehicle number to be used later for vessel list
      src_data_name <- 'vessel_list_imo_master'
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("ALTER TABLE ",input_schema,".vessel_list_imo_master DROP COLUMN rank;
                                        
                                        DROP TABLE IF EXISTS ",input_schema,".temp_master_vl;
                                        
                                        CREATE TABLE ",input_schema,".temp_master_vl AS
                                        WITH temp_vessel_list AS
                                        (SELECT DISTINCT ais_vl_fsr_vsr_vessel_name, ais_vl_fsr_vsr_imo, fsr_vehicle_number, ais_vl_fsr_vsr_date_position, vessel_name_cleaned,
                                        DENSE_RANK() OVER (PARTITION BY ais_vl_fsr_vsr_imo, fsr_vehicle_number
                                        ORDER BY ais_vl_fsr_vsr_date_position DESC) as rank
                                        FROM ",input_schema,".vessel_list_imo_master)
                                        SELECT *
                                        FROM temp_vessel_list
                                        WHERE rank = 1
                                        ORDER BY ais_vl_fsr_vsr_imo, fsr_vehicle_number, ais_vl_fsr_vsr_date_position;
                                        
                                        
                                        DROP TABLE IF EXISTS ",input_schema,".vessel_list_imo_master;
                                        ALTER TABLE ",input_schema,".temp_master_vl RENAME TO vessel_list_imo_master;
                                        "), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('temp_vessel_list', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      # vacuum the FSR daily data table
      sqlQuery(channel, paste0("VACUUM ",input_schema,".fsr_master_daily_data;
                               VACUUM ",input_schema,".vessel_list_imo_master;"))
      
      # Close the channel
      odbcClose(channel)
      
      
    } else {
      print("We don't have the current day's data hence we dropdown previous day")
      
      # Channel is opened
      channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
      
      # Get the previous day's fsr data of those imo that were tagged
      missing_fsr_data = sqlQuery(channel, paste0("SELECT DISTINCT *
                                                  FROM ",input_schema,".fsr_master_daily_data
                                                  WHERE fsr_selected_date = '",previous_date_formatted,"';"))
      
      # Close the channel
      odbcClose(channel)
      
      # Indicate that the data was copied down
      missing_fsr_data$fsr_dropdown_flag = '1'
      missing_fsr_data$fsr_selected_date = today_date_formatted
      
      # write to temp file
      tempFileStorage <- rawConnection(raw(0),"r+")
      
      write.table(missing_fsr_data,tempFileStorage,sep=",",row.names = F, col.names=F)
      
      filename = paste0("fsr_daily_dump.csv")
      
      # Upload to S3
      put_object(file = rawConnectionValue(tempFileStorage), object = paste0(input_s3_bucket_to_store_data,filename), bucket = input_bucket)
      
      # close temporary connection
      close(tempFileStorage)
      
      # If the channel was closed, it will be reopened again
      channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
      
      # Append the newly created FSR data
      src_data_name <- filename
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("COPY ",input_schema,".fsr_master_daily_data 
                                        FROM '",input_s3_bucket_to_store_data,filename,"'
                                        access_key_id '",input_aws_access_key_id,"' 
                                        secret_access_key '",input_aws_secret_access_key,"' 
                                        NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('fsr_master_daily_data', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU and SAFETY_BAU','", src_data_name,"','", end_ts,"')"))
      
      # vacuum the FSR daily data table
      sqlQuery(channel, paste0("VACUUM ",input_schema,".fsr_master_daily_data;"))
      
      
      # Close the channel
      odbcClose(channel)
      
      
    }
    
    
    
    
    
    
    
    #********************************************** VSR Data *********************************************
    # This section gets the VSR extracts from the S3 bucket, cleans the data and uploads to redshift
    
    
    # Get the date from file name
    # get_date <- substr(VSR_file_name,50,57)
    # get_date
    # 
    # date_formatted <- paste0(substring(get_date,1,4),'-',substring(get_date,5,6),'-',substring(get_date,7,8))
    # date_formatted
    
    date_formatted <- substr(VSR_file_name,19,26)
    
    if (today_date == date_formatted) {
      print("Date is the same, so follow logic") 
      
      
      # Getting the VSR daily extract from the S3 bucket
      # tmp <- tempfile(fileext = ".txt")
      # r <- aws.s3::save_object(bucket = input_bucket, object = paste0(input_s3_bucket_short_path,VSR_file_name), file = tmp)
      # workbook <- read.csv(r, header=T, sep=input_delimiter, skip=input_num_lines_to_skip_VSR)
      # 
      
      workbook <- XLConnect::loadWorkbook(VSR_file_name)
      vsr_data <- readWorksheet(workbook, sheet='Table', header=TRUE,startRow = 8,startCol = 2)
      
      colnames(vsr_data) <- input_vsr_list_cols
      
      
      #Removing duplicates, and empty rows
      vsr_new <- unique(vsr_data)
      
      
      
      
      # Formatting dates in order to upload to redshift
      vsr_new$ArrvlDate <- as.Date(vsr_new$ArrvlDate,"%d.%m.%Y")
      vsr_new$VoyB.LDate <- as.Date(vsr_new$VoyB.LDate,"%d.%m.%Y")
      vsr_new$DepDate <- as.Date(vsr_new$DepDate,"%d.%m.%Y")
      vsr_new$FirstVoyTECODate <- as.Date(vsr_new$FirstVoyTECODate,"%d.%m.%Y")
      vsr_new$VoyTECODate <- as.Date(vsr_new$VoyTECODate,"%d.%m.%Y")
      
      
      
      
      
      
      # Adding Selected Date to the data
      vsr_new$SelectedDate = as.Date(date_formatted,format = "%Y-%m-%d")
      
      # Indication that the data was not copied down
      vsr_new$VSR_Dropdown_Flag = '0'
      
      vsr_new$VSR_last_Updated_On_Date = as.Date(date_formatted,format = "%Y-%m-%d")
      
      # write to temp file
      tempFileStorage <- rawConnection(raw(0),"r+")
      
      write.table(vsr_new,tempFileStorage,sep=",",row.names = F, col.names=F)
      
      filename = paste0("vsr_daily_dump_bobj.csv")
      
      # Upload to S3
      put_object(file = rawConnectionValue(tempFileStorage), object = paste0(input_s3_bucket_to_store_data,filename), bucket = input_bucket)
      
      # close temporary connection
      close(tempFileStorage)
      
      # If the channel was closed, it will be reopened again
      channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
      
      
      src_data_name <- filename
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("COPY ",input_schema,".vsr_master_daily_data 
                                        FROM '",input_s3_bucket_to_store_data,filename,"'
                                        access_key_id '",input_aws_access_key_id,"' 
                                        secret_access_key '",input_aws_secret_access_key,"' 
                                        NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('vsr_master_daily_data', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      
      
      # Append the newly created data on the existing FSR daily data table
      #       sqlQuery(channel, paste0("COPY ",input_schema,".vsr_master_daily_data 
      #                                FROM '",input_s3_bucket_to_store_data,filename,"'
      #                                access_key_id '",input_aws_access_key_id,"' 
      #                                secret_access_key '",input_aws_secret_access_key,"' 
      #                                NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"))
      
      # vacuum the FSR daily data table
      sqlQuery(channel, paste0("VACUUM ",input_schema,".vsr_master_daily_data;"))
      
      # Close the channel
      odbcClose(channel)
      
    } else {
      print("We don't have the current day's data hence we dropdown previous day")
      
      
      # Opening the chanel
      channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
      
      # Get the previous day's vsr data of those imo that were tagged
      src_data_name <- 'vsr_master_daily_data'
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      missing_vsr_data = sqlQuery(channel, paste0("SELECT DISTINCT *
                                                  FROM ",input_schema,".vsr_master_daily_data
                                                  WHERE vsr_selected_date = '",previous_date_formatted,"';"), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('missing_vsr_data', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      # Close the channel
      odbcClose(channel)
      
      # Indication that the data was copied down
      
      missing_vsr_data$vsr_dropdown_flag = '1'
      missing_vsr_data$vsr_selected_date = today_date_formatted
      
      # write to temp file
      tempFileStorage <- rawConnection(raw(0),"r+")
      
      write.table(missing_vsr_data,tempFileStorage,sep=",",row.names = F, col.names=F)
      
      filename = paste0("vsr_daily_dump.csv")
      
      # Upload to S3
      put_object(file = rawConnectionValue(tempFileStorage), object = paste0(input_s3_bucket_to_store_data,filename), bucket = input_bucket)
      
      # close temporary connection
      close(tempFileStorage)
      
      # If the channel was closed, it will be reopened again
      channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
      
      # Append the newly created data on the existing VSR daily data table
      src_data_name <- filename
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("COPY ",input_schema,".vsr_master_daily_data 
                                        FROM '",input_s3_bucket_to_store_data,filename,"'
                                        access_key_id '",input_aws_access_key_id,"' 
                                        secret_access_key '",input_aws_secret_access_key,"' 
                                        NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('vsr_master_daily_data', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      # vacuum the FSR daily data table
      sqlQuery(channel, paste0("VACUUM ",input_schema,".vsr_master_daily_data;"))
      
      # Close the channel
      odbcClose(channel)
      
      
    }
    
    
    
    #********************************************** VL Data *********************************************
    # This section gets the VL extracts from the S3 bucket, cleans the data and uploads to redshift
    
    # Get the date from file name
    # get_date <- substr(VL_file_name,50,57)
    # get_date
    # 
    # date_formatted <- paste0(substring(get_date,1,4),'-',substring(get_date,5,6),'-',substring(get_date,7,8))
    # date_formatted
    date_formatted <- substr(VL_file_name,19,26)
    
    
    if (today_date == date_formatted) {
      print("Date is the same, so follow logic")
      
      # Getting the FSR daily extract from the S3 bucket
      # tmp <- tempfile(fileext = ".txt")
      # r <- aws.s3::save_object(bucket = input_bucket, object = paste0(input_s3_bucket_short_path,VL_file_name), file = tmp)
      # workbook <- read.csv(r, header=T, sep=input_delimiter, skip=input_num_lines_to_skip_VL)
      # 
      workbook <- XLConnect::loadWorkbook(VL_file_name)
      vl_data <- readWorksheet(workbook, sheet= 1, header=TRUE,startRow = 1,startCol = 1)
      
      # restricting to only the first n columns
      vl_data <- data.frame(lapply(vl_data, gsub, pattern="[^0-9A-Za-z .&()@%-]+", replacement = ""))
      #
      
      # Pick only relevant columns
      colnames(vl_data) <- input_vl_list_cols
      vl_data <- vl_data[,1:50]
      # Removing duplicates, and empty rows
      vl <- unique(vl_data)
      
      # Filtering VL data for these ports
      vl_filtered = vl %>% filter(Port %in% input_vl_allowed_ports)
      
      
      # vl_filtered$Previous.ETA.at.Next.LP <- strptime(vl_filtered$Previous.ETA.at.Next.LP,"%d%m%Y%H%M%S")
      # vl_filtered$Current.ETA.at.Next.LP <- strptime(vl_filtered$Current.ETA.at.Next.LP,"%d%m%Y%H%M%S")
      # vl_filtered$Voyage.Created.On <- strptime(vl_filtered$Voyage.Created.On,"%d%m%Y%H%M%S")
      # vl_filtered$Laycan.From <- strptime(vl_filtered$Laycan.From,"%d%m%Y%H%M%S")
      # 
      # vl_filtered$Laycan.To <- strptime(vl_filtered$Laycan.To,"%d%m%Y%H%M%S")
      # vl_filtered$Vessel.ETA <- strptime(vl_filtered$Vessel.ETA,"%d%m%Y%H%M%S")
      # vl_filtered$Arrival.Date <- strptime(vl_filtered$Arrival.Date,"%d%m%Y%H%M%S")
      # vl_filtered$REDEL.Date <- strptime(vl_filtered$REDEL.Date,"%d%m%Y%H%M%S")
      # vl_filtered$ETA.Date..Nomination. <- strptime(vl_filtered$ETA.Date..Nomination.,"%d%m%Y%H%M%S")
      
      # write to temp file
      tempFileStorage <- rawConnection(raw(0),"r+")
      
      write.table(vl_filtered,tempFileStorage,sep=",",row.names = F, col.names=F)
      
      filename_vl_filt_temp = paste0("vl_filter_temp_bobj.csv")
      
      # Upload to S3
      put_object(file = rawConnectionValue(tempFileStorage), object = paste0(input_s3_bucket_to_store_data,filename_vl_filt_temp), bucket = input_bucket)
      
      # close temporary connection
      close(tempFileStorage)
      
      # Opening the channel
      channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
      
      sqlQuery(channel, paste0("TRUNCATE TABLE ",input_schema,".vl_filtered_compile;"))
      
      
      
      
      src_data_name <- filename_vl_filt_temp
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("COPY ",input_schema,".vl_filtered_compile 
                                        FROM '",input_s3_bucket_to_store_data,filename_vl_filt_temp,"'
                                        access_key_id '",input_aws_access_key_id,"' 
                                        secret_access_key '",input_aws_secret_access_key,"' 
                                        NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('vl_filtered_compile', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      
      
      
      # Append the newly created data on the existing FSR daily data table
      #       sqlQuery(channel, paste0("COPY ",input_schema,".vl_filtered_compile 
      #                                FROM '",input_s3_bucket_to_store_data,filename_vl_filt_temp,"'
      #                                access_key_id '",input_aws_access_key_id,"' 
      #                                secret_access_key '",input_aws_secret_access_key,"' 
      #                                NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"))
      
      # I need it in a specific format, hence the listing of column names
      vl_filtered = sqlQuery(channel, paste0("SELECT DISTINCT vl_vessel_number, vl_dwt_group, vl_vessel_name, vl_vessel_dwt, vl_vessel_length, vl_vessel_eta, 
                                             vl_previous_eta_at_next_lp, vl_eta_at_next_lp_change_days, vl_laycan_from, vl_laycan_to, 
                                             vl_document_type, vl_demurrage_amount, vl_hire_rate, vl_ebills, vl_voyage_overall_status, vl_new_fix, 
                                             vl_current_voyage_number, vl_nomination_item_comments, vl_voyage_comments, 
                                             vl_contract_number_buy_description, vl_customer_name, vl_vessel_vetting_date, vl_vessel_vetting_id, 
                                             vl_current_eta_at_next_lp, vl_arrival_date_curr_or_vy, vl_hire_indicator, vl_hire_rate_currency, 
                                             vl_customer, vl_demurrage_currency, vl_previous_voyage_number, vl_voyage_created_on, vl_coa_item, 
                                             vl_contract_number_buy, vl_voyage_operator, vl_buy_contract_company_code, vl_open_lifts, vl_vendor, 
                                             vl_buy_contract_comments, vl_redel_date, vl_vendor_name, vl_contract_number_sell, 
                                             vl_contract_number_sell_description, vl_sell_contract_comments, vl_handedover_nominations, 
                                             vl_nomination_key, vl_nomination_key_item, vl_eta_date_nomination, vl_port, vl_port_name, 
                                             vl_voyage_operator_name, y.ais_vl_fsr_vsr_imo
                                             FROM ",input_schema,".vl_bobj AS x LEFT JOIN ",input_schema,".vessel_list_imo_master AS y
                                             ON TRIM(x.vl_vessel_number) = TRIM(y.fsr_vehicle_number)
                                             WHERE x.vl_vessel_number IS NOT NULL;"))
      
      # Close the channel
      odbcClose(channel)
      
      vl_new <- unique(vl_filtered)
      
      
      vl_new$vl_previous_eta_at_next_lp <- strptime(as.character(vl_new$vl_previous_eta_at_next_lp),"%d.%m.%Y %H%M%S")
      vl_new$vl_current_eta_at_next_lp <- strptime(as.character(vl_new$vl_current_eta_at_next_lp),"%d.%m.%Y %H%M%S")
      vl_new$vl_voyage_created_on <- strptime(as.character(vl_new$vl_voyage_created_on),"%d.%m.%Y %H%M%S")
      vl_new$vl_laycan_from <- strptime(as.character(vl_new$vl_laycan_from),"%d.%m.%Y %H%M%S")
      
      vl_new$vl_laycan_to <- strptime(as.character(vl_new$vl_laycan_to),"%d.%m.%Y %H%M%S")
      vl_new$vl_vessel_eta <- strptime(as.character(vl_new$vl_vessel_eta),"%d.%m.%Y %H%M%S")
      vl_new$vl_arrival_date_curr_or_vy <- strptime(as.character(vl_new$vl_arrival_date_curr_or_vy),"%d.%m.%Y %H%M%S")
      vl_new$vl_redel_date <- strptime(as.character(vl_new$vl_redel_date),"%d.%m.%Y %H%M%S")
      vl_new$vl_eta_date_nomination <- strptime(as.character(vl_new$vl_eta_date_nomination),"%d.%m.%Y %H%M%S")

      
     
      
      
      # Adding Selected Date to the data
      vl_new$SelectedDate = as.Date(date_formatted,format = "%Y-%m-%d")
      
      # Adding Arrival Date to be 30 days from the Selected Date
      vl_new$ArrivalDate = vl_new$SelectedDate + 30
      
      vl_new$vl_vessel_vetting_date <- substring(vl_new$vl_vessel_vetting_date, 1, 10)
      
      # Indication that the data was not copied down
      vl_new$VL_Dropdown_Flag = '0'
      vl_new$VL_last_Updated_On_Date = as.Date(date_formatted,format = "%Y-%m-%d")
      
      # write to temp file
      tempFileStorage <- rawConnection(raw(0),"r+")
      
      write.table(vl_new,tempFileStorage,sep=",",row.names = F, col.names=F)
      
      filename = paste0("vl_daily_dump_bobj2.csv")
      
      
      # Upload to S3
      put_object(file = rawConnectionValue(tempFileStorage), object = paste0(input_s3_bucket_to_store_data,filename), bucket = input_bucket)
      
      # close temporary connection
      close(tempFileStorage)
      
      # Opening the channel
      channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
      
      
      
      src_data_name <- filename
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("COPY ",input_schema,".vl_master_daily_data 
                                        FROM '",input_s3_bucket_to_store_data,filename,"'
                                        access_key_id '",input_aws_access_key_id,"' 
                                        secret_access_key '",input_aws_secret_access_key,"' 
                                        NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('vl_master_daily_data', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      
      
      
      
      
      
      # Append the newly created data on the existing FSR daily data table
      #       sqlQuery(channel, paste0("COPY ",input_schema,".vl_master_daily_data 
      #                                FROM '",input_s3_bucket_to_store_data,filename,"'
      #                                access_key_id '",input_aws_access_key_id,"' 
      #                                secret_access_key '",input_aws_secret_access_key,"' 
      #                                NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"))
      
      # vacuum the FSR daily data table
      sqlQuery(channel, paste0("VACUUM ",input_schema,".vl_master_daily_data;"))
      
      
      # Close the channel
      odbcClose(channel)
      
    } else {
      print("We don't have the current day's data hence we dropdown previous day")
      
      
      # If the channel was closed, it will be reopened again
      channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
      
      # Get the previous day's VL data of those imo that were tagged
      missing_vl_data = sqlQuery(channel, paste0("SELECT DISTINCT *
                                                 FROM ",input_schema,".vl_master_daily_data
                                                 WHERE vl_selected_date = '",previous_date_formatted,"';"))
      
      # Close the channel
      odbcClose(channel)
      
      # Indication that the data was copied down
      missing_vl_data$vl_dropdown_flag = '1'
      missing_vl_data$vl_selected_date = today_date_formatted
      
      # write to temp file
      tempFileStorage <- rawConnection(raw(0),"r+")
      
      write.table(missing_vl_data,tempFileStorage,sep=",",row.names = F, col.names=F)
      
      filename = paste0("vl_daily_dump.csv")
      
      # Upload to S3
      put_object(file = rawConnectionValue(tempFileStorage), object = paste0(input_s3_bucket_to_store_data,filename), bucket = input_bucket)
      
      # close temporary connection
      close(tempFileStorage)
      
      # Channel is opened
      channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
      
      # Append the newly created data on the existing VL daily data table
      src_data_name <- filename
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      sqlQuery(channel, paste0("COPY ",input_schema,".vl_master_daily_data 
                               FROM '",input_s3_bucket_to_store_data,filename,"'
                               access_key_id '",input_aws_access_key_id,"' 
                               secret_access_key '",input_aws_secret_access_key,"' 
                               NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('vl_master_daily_data', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      # vacuum the FSR daily data table
      sqlQuery(channel, paste0("VACUUM ",input_schema,".vl_master_daily_data;"))
      
      
      # Close the channel
      odbcClose(channel)
      
    }
    
    
    
    #################################### Section 2: Daily AIS Ingestion
    
    
    #********************************************** AIS Data *********************************************
    # This section gets the AIS data, formats the destinations, adds the necessary merge keys and uploads to redshift
    
    
    # If the channel was closed, it will be reopened again
    channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
    
    # Get the latest ais data, format the destinations,  add necessary merges keys and upload to redshift
    src_data_name <- 'ais_master_daily_data'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("DROP TABLE IF EXISTS ",input_schema,".ais_data_daily_temp;
                                      
                                      CREATE TABLE ",input_schema,".ais_data_daily_temp AS
                                      SELECT DISTINCT aisstatic_name as ais_static_name,
                                      aisstatic_imo as ais_static_imo,
                                      aisstatic_callsign as ais_static_callsign,
                                      aisstatic_flag as ais_static_flag,
                                      aisstatic_length as ais_static_length,
                                      aisstatic_width as ais_static_width,
                                      aisshiptype as ais_shiptype,
                                      aisstatic_updatetime as ais_static_updatetime,
                                      aisstatic_dima as ais_static_dima,
                                      aisstatic_dimb as ais_static_dimb,
                                      aisstatic_dimc as ais_static_dimc,
                                      aisstatic_dimd as ais_static_dimd,
                                      aisstatic_mmsi as ais_static_mmsi,
                                      aisvoyage_updatetime as ais_voyage_updatetime,
                                      aisvoyage_eta as ais_voyage_eta,
                                      aisvoyage_dest as ais_voyage_dest,
                                      aisvoyage_draught as ais_voyage_draught,
                                      aisvoyage_source as ais_voyage_source,
                                      aisposition_timereceived as ais_position_timereceived,
                                      aisposition_src as ais_position_src,
                                      aisposition_lon::float as ais_position_lon,
                                      aisposition_lat::float as ais_position_lat,
                                      aisposition_sog as ais_position_sog,
                                      aisposition_cog as ais_position_cog,
                                      aisposition_hdg as ais_position_hdg,
                                      aisposition_rot as ais_position_rot,
                                      aisposition_navstatus as ais_position_navstatus,
                                      voyagedetails_destination as ais_voyagedetails_destination, --use this for destination formatting per instruction FROM ",input_schema,".vessel tracker
                                      voyagedetails_locode as ais_voyagedetails_locode,
                                      voyagedetails_portcountry as ais_voyagedetails_portcountry,
                                      geodetails_currentport as ais_geodetails_currentport,
                                      geodetails_currentportlocode as ais_geodetails_currentportlocode,
                                      geodetails_currentberth as ais_geodetails_currentberth,
                                      geodetails_timeofatchange as ais_geodetails_timeofatchange,
                                      geodetails_status as ais_geodetails_status,
                                      geodetails_portcountry as ais_geodetails_portcountry,
                                      geodetails_currentanchorage as ais_geodetails_currentanchorage,
                                      aisstatic_updatetime_f as ais_static_updatetime_f,
                                      aisvoyage_updatetime_f as ais_voyage_updatetime_f,
                                      aisvoyage_eta_f as ais_voyage_eta_f,
                                      aisposition_timereceived_f as ais_position_timereceived_f,
                                      date_voyage as ais_date_voyage,
                                      date_position as ais_data_received_date,
                                      date_eta as ais_date_eta,
                                      date_extracted as ais_last_updated_on_date,
                                      date_extracted as ais_date_position,
                                      d_flag as d_flag
                                      FROM ",input_schema,".ais_master_daily_data
                                      WHERE date_extracted = '",today_date_formatted,"'
                                      AND d_flag = 1;
                                      
                                      ALTER TABLE ",input_schema,".ais_data_daily_temp ADD COLUMN ais_dropdown_flag INTEGER;
                                      UPDATE ",input_schema,".ais_data_daily_temp
                                      SET ais_dropdown_flag = '0';
                                      
                                      -- Formatting of destinations
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".temp_DAILY;
                                      
                                      CREATE TABLE ",input_schema,".temp_DAILY as
                                      select a.*, b.destination_formatted as ais_destination_f 
                                      FROM ",input_schema,".ais_data_daily_temp as a
                                      left JOIN 
                                      (WITH destinations_ranked AS
                                      (SELECT *, DENSE_RANK() OVER (PARTITION BY lower(destination)
                                      ORDER BY destination_formatted ASC) as rank
                                      FROM ",input_schema,".ais_destinations_master)
                                      SELECT destination, destination_formatted
                                      FROM destinations_ranked
                                      WHERE rank = 1) as b
                                      on lower(a.ais_voyagedetails_destination) = lower(b.destination);
                                      
                                      ALTER TABLE ",input_schema,".temp_DAILY
                                      rename column ais_voyagedetails_destination to ais_destination_old;
                                      
                                      ALTER TABLE ",input_schema,".temp_DAILY 
                                      rename column ais_destination_f to ais_destination;
                                      
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".ais_data_daily_temp;
                                      
                                      ALTER TABLE ",input_schema,".temp_DAILY 
                                      rename to ais_data_daily_temp;
                                      
                                      
                                      
                                      -- Populate values for the keys for AIS
                                      
                                      ALTER TABLE ",input_schema,".ais_data_daily_temp ADD COLUMN AIS_date_key varchar(200);
                                      
                                      UPDATE ",input_schema,".ais_data_daily_temp
                                      set AIS_date_key = concat(AIS_static_imo,AIS_date_position)
                                      where not (AIS_static_imo = '0' or AIS_static_imo IS NULL) ;
                                      
                                      
                                      INSERT INTO ",input_schema,".AIS_DATA_DAILY
                                      SELECT DISTINCT *
                                      FROM ",input_schema,".ais_data_daily_temp;
                                      
                                      VACUUM ",input_schema,".AIS_DATA_DAILY;
                                      "), FALSE)
    
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('ais_data_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    
    
    missing_ais_data = sqlQuery(channel, paste0("SELECT DISTINCT *
                                                FROM ",input_schema,".AIS_DATA_DAILY
                                                WHERE ais_date_position = '",previous_date_formatted,"' 
                                                AND ais_static_imo IN (SELECT DISTINCT ais_static_imo
                                                FROM
                                                (SELECT DISTINCT ais_static_imo 
                                                FROM ",input_schema,".AIS_DATA_DAILY
                                                WHERE ais_date_position = '",previous_date_formatted,"' 
                                                AND ais_static_imo IS NOT NULL)  
                                                
                                                EXCEPT
                                                
                                                (SELECT DISTINCT ais_static_imo 
                                                FROM ",input_schema,".AIS_DATA_DAILY
                                                WHERE ais_date_position = '",today_date_formatted,"' 
                                                AND ais_static_imo IS NOT NULL));"))
    
    # Close the channel
    odbcClose(channel)
    
    nrow(missing_ais_data)
    
    if (nrow(missing_ais_data) > 0) { 
      #Indication that the data was copied down
      missing_ais_data$ais_dropdown_flag = '1'
      missing_ais_data$ais_date_position = today_date_formatted
      missing_ais_data$ais_date_key = paste0(missing_ais_data$ais_static_imo,today_date_formatted)
      
      # write to temp file
      tempFileStorage <- rawConnection(raw(0),"r+")
      
      write.table(missing_ais_data,tempFileStorage,sep=",",row.names = F, col.names=F)
      
      filename_ais = paste0("ais_daily_dump.csv")
      
      # Upload to S3
      put_object(file = rawConnectionValue(tempFileStorage), object = paste0(input_s3_bucket_to_store_data,filename_ais), bucket = input_bucket)
      
      # close temporary connection
      close(tempFileStorage)
      
      # Channel is opened
      channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
      
      src_data_name <- filename_ais
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("COPY ",input_schema,".ais_data_daily_temp
                                        FROM '",input_s3_bucket_to_store_data,filename_ais,"'
                                        access_key_id '",input_aws_access_key_id,"' 
                                        secret_access_key '",input_aws_secret_access_key,"' 
                                        NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('ais_data_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      
      # Append the newly created data on the existing AIS daily data table
      #       sqlQuery(channel, paste0("COPY ",input_schema,".ais_data_daily_temp
      #                                FROM '",input_s3_bucket_to_store_data,filename_ais,"'
      #                                access_key_id '",input_aws_access_key_id,"' 
      #                                secret_access_key '",input_aws_secret_access_key,"' 
      #                                NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"))
      
      src_data_name <- filename_ais
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("COPY ",input_schema,".AIS_DATA_DAILY
                                        FROM '",input_s3_bucket_to_store_data,filename_ais,"'
                                        access_key_id '",input_aws_access_key_id,"' 
                                        secret_access_key '",input_aws_secret_access_key,"' 
                                        NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('ais_data_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      
      
      
      # Append the newly created data on the existing AIS daily data table
      #       sqlQuery(channel, paste0("COPY ",input_schema,".AIS_DATA_DAILY
      #                                FROM '",input_s3_bucket_to_store_data,filename_ais,"'
      #                                access_key_id '",input_aws_access_key_id,"' 
      #                                secret_access_key '",input_aws_secret_access_key,"' 
      #                                NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"))
      
      # vacuum the AIS daily data table
      sqlQuery(channel, paste0("VACUUM ",input_schema,".AIS_DATA_DAILY;"))
      
      # Close the channel
      odbcClose(channel)
    }
    
    
    
    
    
    #sys.sleep(10)
    
    
    #**************************************Filename: 2.  S5 - Future State 20171016.R
    # print("2. S5 - Future State 20171016.R")
    print(paste0("2. S5 - Future State ",today_date,".R"))
    
    
    # Description: 
    #  - This R code gets the daily S5 Iron Ore and S5 Coal extracts. (automatically finds the latest file)
    #  - The code will only be executed for the days when S5 data is available
    #  - Formats and adds the necessary columns
    #  - Appends the final data to the existing tables
    
    # Dependencies: 
    #   1: The file names must be in the following formats for the respective files for this code to work
    #       a) yyyymmddVM.xlsx (For S5 Iron Ore)
    #       a) COAL - mmm yyyy.xlsx (For S5 Coal)
    #   2: The extracts must be in the S3 bucket with the following file path: 'BHP_PwC/7. Daily extracts'
    #       - If not please go to *inputs* section at the top and change the path
    #   3: Username used to connect to redshift is dapappadmin (hence this user must be given access in order for this to run.)
    # 
    # Assumptions: NA
    
    
    #################################### Section 3: Daily S5 Ingestion
    
    # Start value
    S5_Iron_Ore_file_name = paste0(input_s3_bucket_to_read_raw_files,"IRON ORE - 20170101VM.xlsx")
    S5_Coal_file_name = paste0(input_s3_bucket_to_read_raw_files,"COAL - 20170101VM.xlsx")
    
    
    # Loop through all files for S5 Iron Ore and Coal to find the latest file for each
    for (file in bucket){
      
      # S5 Iron Ore
      if(substr(file$Key,27,34) == 'IRON ORE') {
        S5_Iron_Ore_Latest_File = substr(file$Key,38,45)
        if(S5_Iron_Ore_Latest_File == date_stripped){
          S5_Iron_Ore_file_name = file$Key
        }
      }
      
      
      # S5 Coal
      if(substr(file$Key,27,30) == 'COAL') {
        S5_Coal_Latest_File = substr(file$Key,34,41)
        if(S5_Coal_Latest_File == date_stripped){
          S5_Coal_file_name = file$Key
        }
      }
      
    }
    
    print(S5_Iron_Ore_file_name)
    print(S5_Coal_file_name)
    
    
    
    
    #------------------------------------ Iron Ore
    
    file_date_iron <- paste0(substr(S5_Iron_Ore_file_name,38,41),"-",substr(S5_Iron_Ore_file_name,42,43),"-",substr(S5_Iron_Ore_file_name,44,45))
    
    file_date_iron
    
    if (today_date == file_date_iron){
      print("S5 Iron Ore - Today's Data is available hence insert to redshift")
      
      tmp <- tempfile(fileext = ".xlsx")
      r <- aws.s3::save_object(bucket = input_bucket, object = paste0(input_s3_bucket_short_path,S5_Iron_Ore_file_name), file = tmp)
      workbook <- XLConnect::loadWorkbook(r)
      s5_iron_data <- readWorksheet(workbook, sheet=1, header=TRUE)
      
      # Pick only relevant columns
      s5_iron_data <- s5_iron_data[,input_s5_iron_ore_list_cols]
      
      
      data_s5_iron <- unique(s5_iron_data)
      
      #Handling characters inside the dataset
      data_s5_iron <- as.data.frame(apply(data_s5_iron,2,function(x)gsub('#', NA,x)))
      data_s5_iron <- as.data.frame(apply(data_s5_iron,2,function(x)gsub('N/A', NA,x)))
      data_s5_iron <- as.data.frame(apply(data_s5_iron,2,function(x)gsub('"', '',x)))
      
      #Special case for removing question mark
      listQuestionMark <- data_s5_iron == '?'
      #listQuestionMark
      
      is.na(data_s5_iron) <- listQuestionMark
      
      #Changing the column names
      names(data_s5_iron)[1]<-paste("S5_Ref_Number")
      names(data_s5_iron)[3]<-paste("Voyage_Number")
      
      data_s5_iron$S5_Ref_Number<-as.character(data_s5_iron$S5_Ref_Number)
      
      
      #Adding additional columns
      data_s5_iron['Vessel_Status'] <- "NA"
      data_s5_iron['Date_Position'] <- file_date_iron
      data_s5_iron['DayOfMonth'] <- substr(file_date_iron,9,10)
      
      data_s5_iron['Workbook Name'] <- substr(S5_Iron_Ore_file_name,27,52)
      data_s5_iron['S5_source_commodity'] <- "Iron Ore" #may need to change this 
      data_s5_iron['s5_vessel_name_cleansed'] <- ""
      data_s5_iron['s5_vessel_name_date_key'] <- ""
      data_s5_iron['s5_operator_raw'] <- ""
      
      
      
      rowsToRemove <- numeric()
      
      #runs through and finds the rows to remove (as there are blank rows)
      for (i in 1:nrow(data_s5_iron)){
        row <- data_s5_iron[i,]
        s5Ref <- row[1]
        operator <- row[2]
        
        
        if((!is.na(s5Ref)) && is.na(operator)){
          vesselStatus <- s5Ref
          rowsToRemove <- append(rowsToRemove,i)
        }
        data_s5_iron[i,'Vessel_Status'] <- vesselStatus
      }
      
      data_s5_iron <- data_s5_iron[-rowsToRemove, ]
      
      
      data_s5_iron <- data_s5_iron[ ,c(16:18,1:15,19:23)] #reordering the columns
      
      
      #Rename the columns
      names(data_s5_iron)[1] <- "vessel_status"
      names(data_s5_iron)[2] <- "date_position"
      names(data_s5_iron)[3] <- "dayofmonth"
      names(data_s5_iron)[4] <- "s5_ref_number"
      names(data_s5_iron)[5] <- "operator"
      names(data_s5_iron)[6] <- "voyage_number"
      names(data_s5_iron)[7] <- "vessel_name"
      names(data_s5_iron)[8] <- "port"
      names(data_s5_iron)[9] <- "eta"
      names(data_s5_iron)[10] <- "etb"
      names(data_s5_iron)[11] <- "etd"
      names(data_s5_iron)[12] <- "remark"
      names(data_s5_iron)[13] <- "loi_accepted"
      names(data_s5_iron)[14] <- "risk_of_delay_to_berth"
      names(data_s5_iron)[15] <- "reason"
      names(data_s5_iron)[16] <- "variance_t1"
      names(data_s5_iron)[17] <- "eta_port_hedland"
      names(data_s5_iron)[18] <- "receiver"
      names(data_s5_iron)[19] <- "workbook_name"
      names(data_s5_iron)[20] <- "s5_source_commodity"
      names(data_s5_iron)[21] <- "s5_vessel_name_cleansed"
      names(data_s5_iron)[22] <- "s5_vessel_name_date_key"
      names(data_s5_iron)[23] <- "s5_operator_raw"
      
      # Runs through the file to remove text in date columns
      for (i in 1:nrow(data_s5_iron)){
        eta <- toString(data_s5_iron[i,9])
        etb <- toString(data_s5_iron[i,10])
        etd <- toString(data_s5_iron[i,11])
        
        
        if(!is.na(eta) && grepl("[A-Za-z]",eta)){
          data_s5_iron[i,9] <- NA
        }
        
        if(!is.na(etb) && grepl("[A-Za-z]",etb)){
          data_s5_iron[i,10] <- NA
        }
        
        if(!is.na(etd) && grepl("[A-Za-z]",etd)){
          data_s5_iron[i,11] <- NA
        }
      }
      
      # write to temp file
      tempFileStorage <- rawConnection(raw(0),"r+")
      
      write.table(data_s5_iron,tempFileStorage,sep=",",row.names = F, col.names=F)
      
      filename_s5_iron_daily = paste0("s5_daily.csv")
      
      # Upload to S3
      put_object(file = rawConnectionValue(tempFileStorage), object = paste0(input_s3_bucket_to_store_data,filename_s5_iron_daily), bucket = input_bucket)
      
      # close temporary connection
      close(tempFileStorage)
      
      
      # Channel is opened
      channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
      
      # Get the imo based on vessel name, create merge keys and then upload to redshift
      sqlQuery(channel, paste0("TRUNCATE TABLE ",input_schema,".s5_historical_dump_daily_compile;")) 
      
      src_data_name <- filename_s5_iron_daily
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("COPY ",input_schema,".s5_historical_dump_daily_compile 
                                        FROM '",input_s3_bucket_to_store_data,filename_s5_iron_daily,"'
                                        access_key_id '",input_aws_access_key_id,"' 
                                        secret_access_key '",input_aws_secret_access_key,"' 
                                        NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('s5_historical_dump_daily_compile', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      
      
      src_data_name <- 's5_historical_dump_daily_compile'
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("UPDATE ",input_schema,".s5_historical_dump_daily_compile
                                        SET s5_operator_raw = s5_operator;
                                        
                                        UPDATE ",input_schema,".s5_historical_dump_daily_compile
                                        SET s5_operator = (CASE
                                        WHEN s5_operator_raw = '0' THEN NULL
                                        WHEN s5_operator_raw = ' ' THEN NULL
                                        WHEN REGEXP_REPLACE(UPPER(s5_operator_raw), '[^a-zA-Z0-9]+', '') = 'LIA' THEN 'Lisa'
                                        WHEN REGEXP_REPLACE(UPPER(s5_operator_raw), '[^a-zA-Z0-9]+', '') = 'YEETAT' THEN 'Yee Tat'
                                        WHEN REGEXP_REPLACE(UPPER(s5_operator_raw), '[^a-zA-Z0-9]+', '') = 'YENLING' THEN 'Yen Ling'
                                        WHEN REGEXP_REPLACE(UPPER(s5_operator_raw), '[^a-zA-Z0-9]+', '') = 'CAOJING' THEN 'Cao Jing'
                                        WHEN REGEXP_REPLACE(UPPER(s5_operator_raw), '[^a-zA-Z0-9]+', '') = 'HUIYI' THEN 'Hui Yi'
                                        WHEN REGEXP_REPLACE(UPPER(s5_operator_raw), '[^a-zA-Z0-9]+', '') = 'MEICHEE' THEN 'Mei Chee'
                                        WHEN REGEXP_REPLACE(UPPER(s5_operator_raw), '[^a-zA-Z0-9]+', '') = 'SIMIN' THEN 'Si Min'
                                        WHEN REGEXP_REPLACE(UPPER(s5_operator_raw), '[^a-zA-Z0-9]+', '') = 'YILEE' THEN 'Yi Lee'
                                        ELSE s5_operator_raw
                                        END);
                                        
                                        ALTER TABLE ",input_schema,".s5_historical_dump_daily_compile ADD COLUMN s5_port_raw VARCHAR(200);
                                        
                                        UPDATE ",input_schema,".s5_historical_dump_daily_compile
                                        SET s5_port_raw = s5_port;
                                        
                                        UPDATE ",input_schema,".s5_historical_dump_daily_compile
                                        SET s5_port = (CASE
                                        WHEN s5_port_raw = '0' THEN NULL
                                        WHEN s5_port_raw = ' ' THEN NULL
                                        ELSE s5_port_raw
                                        END);
                                        
                                        ALTER TABLE ",input_schema,".s5_historical_dump_daily_compile DROP COLUMN s5_port_raw;
                                        
                                        -- Cleansing vessel name
                                        UPDATE ",input_schema,".s5_historical_dump_daily_compile
                                        SET s5_vessel_name_cleansed = REGEXP_REPLACE(UPPER(s5_vessel_name), '[^a-zA-Z0-9]+', '');
                                        
                                        --- making the date key
                                        UPDATE ",input_schema,".s5_historical_dump_daily_compile
                                        SET s5_vessel_name_date_key = CONCAT(s5_vessel_name_cleansed,s5_date_position);
                                        
                                        INSERT INTO ",input_schema,".s5_historical_dump_daily
                                        SELECT DISTINCT *
                                        FROM ",input_schema,".s5_historical_dump_daily_compile;"), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('s5_historical_dump_daily_compile', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      # Close the channel
      odbcClose(channel)
      
    } else {
      print("S5 Iron Ore - Today's Data is NOT available hence no action is performed")
    }
    
    S5_Coal_file_name
    
    #------------------------------------ COAL
    
    
    
    file_date_coal <- paste0(substr(S5_Coal_file_name,34,37),"-",substr(S5_Coal_file_name,38,39),"-",substr(S5_Coal_file_name,40,41))
    file_date_coal
    
    if (today_date == file_date_coal){
      print("S5 Coal - Today's Data is available hence insert to redshift")
      
      tmp <- tempfile(fileext = ".xlsx")
      r <- aws.s3::save_object(bucket = input_bucket, object = paste0(input_s3_bucket_short_path,S5_Coal_file_name), file = tmp)
      
      workbook <- XLConnect::loadWorkbook(r)
      s5_coal_data <- readWorksheet(workbook, sheet=1, header=TRUE)
      
      # Pick only relevant columns
      s5_coal_data <- s5_coal_data[,input_s5_coal_list_cols]
      
      data_s5_coal <- unique(s5_coal_data)
      
      #Handling characters inside the dataset
      data_s5_coal <- as.data.frame(apply(data_s5_coal,2,function(x)gsub('#', NA,x)))
      data_s5_coal <- as.data.frame(apply(data_s5_coal,2,function(x)gsub('N/A', NA,x)))
      data_s5_coal <- as.data.frame(apply(data_s5_coal,2,function(x)gsub('"', '',x)))
      
      #Special case for removing question mark
      listQuestionMark <- data_s5_iron == '?'
      #listQuestionMark
      
      is.na(data_s5_iron) <- listQuestionMark
      
      #Changing the column names
      names(data_s5_coal)[1]<-paste("S5_Ref_Number")
      names(data_s5_coal)[3]<-paste("Voyage_Number")
      
      data_s5_coal$S5_Ref_Number<-as.character(data_s5_coal$S5_Ref_Number)
      
      
      #Adding additional columns
      data_s5_coal['Vessel_Status'] <- "NA"
      data_s5_coal['Date_Position'] <- file_date_coal
      data_s5_coal['DayOfMonth'] <- substr(file_date_coal,9,10)
      
      
      # this is dependent on the file name, coal comes without date, so we insert the date inside the workbook name
      data_s5_coal['Workbook Name'] <- substr(S5_Coal_file_name,27,48)
      data_s5_coal['S5_source_commodity'] <- "Coal"
      data_s5_coal['s5_vessel_name_cleansed'] <- ""
      data_s5_coal['s5_vessel_name_date_key'] <- ""
      data_s5_coal['s5_operator_raw'] <- ""
      
      rowsToRemove <- numeric()
      
      #runs through and finds the rows to remove (as there are blank rows)
      for (i in 1:nrow(data_s5_coal)){
        row <- data_s5_coal[i,]
        s5Ref <- row[1]
        operator <- row[2]
        
        if((!is.na(s5Ref)) && is.na(operator)){
          vesselStatus <- s5Ref
          rowsToRemove <- append(rowsToRemove,i)
        }
        data_s5_coal[i,'Vessel_Status'] <- vesselStatus
      }
      
      data_s5_coal <- data_s5_coal[-rowsToRemove, ]
      
      data_s5_coal <- data_s5_coal[ ,c(16:18,1:15,19:23)] #reordering the columns
      
      #Rename the columns
      names(data_s5_coal)[1] <- "vessel_status"
      names(data_s5_coal)[2] <- "date_position"
      names(data_s5_coal)[3] <- "dayofmonth"
      names(data_s5_coal)[4] <- "s5_ref_number"
      names(data_s5_coal)[5] <- "operator"
      names(data_s5_coal)[6] <- "voyage_number"
      names(data_s5_coal)[7] <- "vessel_name"
      names(data_s5_coal)[8] <- "port"
      names(data_s5_coal)[9] <- "eta"
      names(data_s5_coal)[10] <- "etb"
      names(data_s5_coal)[11] <- "etd"
      names(data_s5_coal)[12] <- "remark"
      names(data_s5_coal)[13] <- "loi_accepted"
      names(data_s5_coal)[14] <- "risk_of_delay_to_berth"
      names(data_s5_coal)[15] <- "reason"
      names(data_s5_coal)[16] <- "variance_t1"
      names(data_s5_coal)[17] <- "eta_port_hedland"
      names(data_s5_coal)[18] <- "receiver"
      names(data_s5_coal)[19] <- "workbook_name"
      names(data_s5_coal)[20] <- "s5_source_commodity"
      names(data_s5_coal)[21] <- "s5_vessel_name_cleansed"
      names(data_s5_coal)[22] <- "s5_vessel_name_date_key"
      names(data_s5_coal)[23] <- "s5_operator_raw"
      
      # Runs through the file to remove text in date columns
      for (i in 1:nrow(data_s5_coal)){
        eta <- toString(data_s5_coal[i,9])
        etb <- toString(data_s5_coal[i,10])
        etd <- toString(data_s5_coal[i,11])
        
        
        if(!is.na(eta) && grepl("[A-Za-z]",eta)){
          data_s5_coal[i,9] <- NA
        }
        
        if(!is.na(etb) && grepl("[A-Za-z]",etb)){
          data_s5_coal[i,10] <- NA
        }
        
        if(!is.na(etd) && grepl("[A-Za-z]",etd)){
          data_s5_coal[i,11] <- NA
        }
      }
      
      # write to temp file
      tempFileStorage <- rawConnection(raw(0),"r+")
      
      write.table(data_s5_coal,tempFileStorage,sep=",",row.names = F, col.names=F)
      
      filename_s5_coal_daily = paste0("s5_daily_coal.csv")
      
      # Upload to S3
      put_object(file = rawConnectionValue(tempFileStorage), object = paste0(input_s3_bucket_to_store_data,filename_s5_coal_daily), bucket = input_bucket)
      
      # close temporary connection
      close(tempFileStorage)
      
      
      # Channel is opened
      channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
      
      # # Get the imo based on vessel name, create merge keys and then upload to redshift
      sqlQuery(channel, paste0("TRUNCATE TABLE ",input_schema,".s5_historical_dump_daily_compile;"))
      src_data_name <- filename_s5_coal_daily
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("COPY ",input_schema,".s5_historical_dump_daily_compile 
                                        FROM '",input_s3_bucket_to_store_data,filename_s5_coal_daily,"'
                                        access_key_id '",input_aws_access_key_id,"' 
                                        secret_access_key '",input_aws_secret_access_key,"' 
                                        NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('s5_historical_dump_daily_compile', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      
      src_data_name <- 's5_historical_dump_daily_compile'
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("UPDATE ",input_schema,".s5_historical_dump_daily_compile
                                        SET s5_operator_raw = s5_operator;
                                        
                                        UPDATE ",input_schema,".s5_historical_dump_daily_compile
                                        SET s5_operator = (CASE
                                        WHEN s5_operator_raw = '0' THEN NULL
                                        WHEN s5_operator_raw = ' ' THEN NULL
                                        WHEN REGEXP_REPLACE(UPPER(s5_operator_raw), '[^a-zA-Z0-9]+', '') = 'LIA' THEN 'Lisa'
                                        WHEN REGEXP_REPLACE(UPPER(s5_operator_raw), '[^a-zA-Z0-9]+', '') = 'YEETAT' THEN 'Yee Tat'
                                        WHEN REGEXP_REPLACE(UPPER(s5_operator_raw), '[^a-zA-Z0-9]+', '') = 'YENLING' THEN 'Yen Ling'
                                        WHEN REGEXP_REPLACE(UPPER(s5_operator_raw), '[^a-zA-Z0-9]+', '') = 'CAOJING' THEN 'Cao Jing'
                                        WHEN REGEXP_REPLACE(UPPER(s5_operator_raw), '[^a-zA-Z0-9]+', '') = 'HUIYI' THEN 'Hui Yi'
                                        WHEN REGEXP_REPLACE(UPPER(s5_operator_raw), '[^a-zA-Z0-9]+', '') = 'MEICHEE' THEN 'Mei Chee'
                                        WHEN REGEXP_REPLACE(UPPER(s5_operator_raw), '[^a-zA-Z0-9]+', '') = 'SIMIN' THEN 'Si Min'
                                        WHEN REGEXP_REPLACE(UPPER(s5_operator_raw), '[^a-zA-Z0-9]+', '') = 'YILEE' THEN 'Yi Lee'
                                        ELSE s5_operator_raw
                                        END);
                                        
                                        ALTER TABLE ",input_schema,".s5_historical_dump_daily_compile ADD COLUMN s5_port_raw VARCHAR(200);
                                        
                                        UPDATE ",input_schema,".s5_historical_dump_daily_compile
                                        SET s5_port_raw = s5_port;
                                        
                                        UPDATE ",input_schema,".s5_historical_dump_daily_compile
                                        SET s5_port = (CASE
                                        WHEN s5_port_raw = '0' THEN NULL
                                        WHEN s5_port_raw = ' ' THEN NULL
                                        ELSE s5_port_raw
                                        END);
                                        
                                        ALTER TABLE ",input_schema,".s5_historical_dump_daily_compile DROP COLUMN s5_port_raw;
                                        
                                        -- Cleansing vessel name
                                        UPDATE ",input_schema,".s5_historical_dump_daily_compile
                                        SET s5_vessel_name_cleansed = REGEXP_REPLACE(UPPER(s5_vessel_name), '[^a-zA-Z0-9]+', '');
                                        
                                        --- making the date key
                                        UPDATE ",input_schema,".s5_historical_dump_daily_compile
                                        SET s5_vessel_name_date_key = CONCAT(s5_vessel_name_cleansed,s5_date_position);
                                        
                                        INSERT INTO ",input_schema,".s5_historical_dump_daily
                                        SELECT DISTINCT *
                                        FROM ",input_schema,".s5_historical_dump_daily_compile;"), FALSE)
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('s5_historical_dump_daily_compile', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      # Close the channel
      odbcClose(channel)
    } else {
      print("S5 Coal - Today's Data is NOT available hence no action is performed")
    }
    
    
    
    #sys.sleep(10)
    
    
    
    #************************************ Filename: 3. Disport Profiler Calculations 20171016.R     
    # print("3. Disport Profiler Calculations 20171016.R")
    print(paste0("3. Disport Profiler Calculations ",today_date,".R"))
    
    # Description: 
    #  - This R code gets the FAR extracts. (automatically finds the latest file)
    #  - The code will only be executed for the days when FAR data is available
    #  - Formats and adds the necessary columns
    #  - Appends the final data to the existing tables
    
    # Dependencies: 
    #   1: The file names must be in the following formats for the respective files for this code to work
    #       a) FAR_extracted yyyymmdd.xlsx
    #   2: The extracts must be in the S3 bucket with the following file path: 'BHP_PwC/7. Daily extracts'
    #       - If not please go to the *inputs* section at the top and change the path
    #   3: Username used to connect to redshift is dapappadmin (hence this user must be given access in order for this to run.)
    # 
    # Assumptions: NA
    
    #################################### Section 4: Fortnightly FAR SAP Ingestion
    
    
    # Start value
    # FAR_file_name = paste0(input_s3_bucket_to_read_raw_files,"FREIGHT_MKT_FAR_PART1OF2_20170101011111.csv")
    FAR_file_name = 'FAR Daily - BOBJ.xlsx'
    date_formatted <- substr(FAR_file_name,19,26)
    
    # Loop through all files for FAR to find the latest file
    # for (file in bucket){
    #   #FAR
    #   if(substr(file$Key,35,50) == 'MKT_FAR_PART1OF2') {
    #     FAR_Latest_File = substr(file$Key,52,59)
    #     if(FAR_Latest_File == date_stripped){
    #       FAR_file_name = file$Key
    #     }
    #   }
    #   
    # }
    # print(FAR_file_name)
    # 
    # get_date <- substr(FAR_file_name,52,59)
    # get_date
    # 
    # date_formatted <- paste0(substring(get_date,1,4),'-',substring(get_date,5,6),'-',substring(get_date,7,8))
    # date_formatted
    
    
    if (today_date == date_formatted){
      print("FAR - Today's Data is available hence insert to redshift")
      
      #Get FAR file from S3 bucket
      #       tmp <- tempfile(fileext = ".xlsx")
      #       r <- aws.s3::save_object(bucket = input_bucket, object = paste0(input_s3_bucket_short_path,FAR_file_name), file = tmp)
      #       workbook <- XLConnect::loadWorkbook(r)
      #       far_data <- readWorksheet(workbook, sheet=1, header=TRUE)
      
      
      workbook <- XLConnect::loadWorkbook(FAR_file_name)
      far_data <- readWorksheet(workbook, sheet= 'Table', header=TRUE,startRow = 10,startCol = 2)
      # workbook <- read.csv(r, header=T, sep=input_delimiter, skip=input_num_lines_to_skip_FAR)
      
      #removing all unnecessary characters
      far_new <- data.frame(lapply(far_data, gsub, pattern="[^0-9A-Za-z .&()@%-]+", replacement = ""))
      
      
      far_new <- unique(far_new)
      
      # Removing the empty rows
      far_new <- far_new[!(is.na(far_new[10]) | far_new[10]==""), ]
      
      #Adding column that is not available
      
      far_new$Discharge_Port_Description = NA
      far_new <- far_new[,c(1:21,30,22:29)]
      
      colnames(far_new) <- input_far_list_cols
      # Pick only relevant columns
      # far_new <- far_new[,input_far_list_cols]
      
      far_new$ActualQuantity <-  sub('\\..*', '', far_new$ActualQuantity)
      
      # Formatting dates
      far_new$SchdDateforNom <- as.Date(far_new$SchdDateforNom,"%d.%m.%Y")
      far_new$LCLPDate <- as.Date(far_new$LCLPDate,"%d.%m.%Y")
      far_new$BOLDate <- as.Date(far_new$BOLDate,"%d.%m.%Y")
      
      far_new$VoyageDurationStart = as.character(as.POSIXct(far_new$VoyageDurationStart, format = "%d.%m.%Y %H%M%S"))
      far_new$VoyageDurationEnd = as.character(as.POSIXct(far_new$VoyageDurationEnd, format = "%d.%m.%Y %H%M%S"))
      far_new$CTContractDate = as.Date(far_new$CTContractDate,"%d.%m.%Y")
      
      far_new$source_file = FAR_file_name
      
      
      
      
      # write to temp file
      tempFileStorage <- rawConnection(raw(0),"r+")
      
      write.table(far_new,tempFileStorage,sep=",",row.names = F, col.names=F)
      
      filename = paste0("far_daily_dump_bobj.csv")
      
      # Upload to S3
      put_object(file = rawConnectionValue(tempFileStorage), object = paste0(input_s3_bucket_to_store_data,filename), bucket = input_bucket)
      
      # close temporary connection
      close(tempFileStorage)
      
      
      # If the channel was closed, it will be reopened again
      channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
      
      # Add the load port descriptions and upload to main far table
      sqlQuery(channel, paste0("TRUNCATE TABLE ",input_schema,".far_disport_profiler_compile;
                               
                               ALTER TABLE ",input_schema,". far_disport_profiler_compile DROP COLUMN load_port_desc;"))
      
     
      
      src_data_name <- filename
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("COPY ",input_schema,".far_disport_profiler_compile
                                        FROM '",input_s3_bucket_to_store_data,filename,"'
                                        access_key_id '",input_aws_access_key_id,"' 
                                        secret_access_key '",input_aws_secret_access_key,"' 
                                        NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('far_disport_profiler_compile', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      
      src_data_name <- 'far_disport_profiler_compile'
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("ALTER TABLE ",input_schema,".far_disport_profiler_compile ADD COLUMN load_port_desc VARCHAR(250);
                                        
                                        /* Adding the load port mapping - don't run if load port mapping is already provided */
                                        UPDATE ",input_schema,".far_disport_profiler_compile
                                        SET load_port_desc = (
                                        case 
                                        when load_port = '168' then 'Port Hedland'
                                        when load_port = '144' then 'Haypoint'
                                        when load_port = '136' then 'Gladstone'
                                        when load_port = '126' then 'Dalrymple Bay'
                                        when load_port = '460' then 'Mejillones'
                                        when load_port = '439' then 'Antofagasta'
                                        when load_port = '450' then 'Caleta Coloso'
                                        when load_port = '456' then 'Iquique'
                                        else 'Unknown'
                                        end);
                                        
                                        
                                        
                                        INSERT INTO ",input_schema,".far_disport_profiler
                                        SELECT *
                                        FROM ",input_schema,".far_disport_profiler_compile;"),FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('far_disport_profiler_compile', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      
      
      
      # Get relevant columns needed for spotfire, add ship type and ship to party name, cleanup commodity and upload to table
      src_data_name <- 'far_disport_profiler_compile'
      start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      error <- sqlQuery(channel, paste0("DROP TABLE IF EXISTS ",input_schema,".far_disport_profiler_daily_temp;
                                        
                                        CREATE TABLE ",input_schema,".far_disport_profiler_daily_temp AS
                                        SELECT DISTINCT freight_voyage, 
                                        nomination_number, 
                                        schd_date_for_nom, 
                                        TRIM(UPPER(load_port_desc)) as load_port_desc, 
                                        TRIM(UPPER(discharge_port_desc)) as discharge_port_desc, 
                                        TRIM(UPPER(commodity)) as commodity, 
                                        TRIM(UPPER(material)) as material, 
                                        actual_quantity
                                        FROM ",input_schema,".far_disport_profiler_compile;
                                        
                                        
                                        /* Creating necessary columns for the calculations - Need to run the code for AIS master data creation before this*/
                                        ALTER TABLE ",input_schema,".far_disport_profiler_daily_temp ADD COLUMN ship_to_party_name VARCHAR(450) default NULL;
                                        ALTER TABLE ",input_schema,".far_disport_profiler_daily_temp ADD COLUMN ship_type VARCHAR(450) default NULL;
                                        
                                        /* Getting ship to party data from t_m_ais_vl_fsr_ru table */
                                        
                                        UPDATE ",input_schema,".far_disport_profiler_daily_temp
                                        SET ship_to_party_name = md.fsr_ship_to_party_name
                                        FROM
                                        (SELECT *
                                        FROM
                                        (SELECT DISTINCT vl_fsr_vsr_nomination_number, TRIM(UPPER(fsr_ship_to_party_name)) AS fsr_ship_to_party_name
                                        FROM ",input_schema,".AIS_VL_FSR_VSR_Daily
                                        WHERE vl_fsr_vsr_nomination_number IS NOT NULL)
                                        UNION
                                        (SELECT DISTINCT fsr_nomination_number, TRIM(UPPER(fsr_ship_to_party_name)) AS fsr_ship_to_party_name
                                        FROM ",input_schema,".m_ais_vl_fsr_ru
                                        WHERE fsr_nomination_number IS NOT NULL)) as md
                                        WHERE t_far_disport_profiler_daily_temp.nomination_number = md.vl_fsr_vsr_nomination_number;
                                        
                                        
                                        
                                        /* Getting ship type from t_m_ais_vl_fsr_ru table */
                                        
                                        UPDATE ",input_schema,".far_disport_profiler_daily_temp
                                        SET ship_type = md.ais_shiptype
                                        FROM
                                        (SELECT *
                                        FROM
                                        (SELECT DISTINCT vl_fsr_vsr_nomination_number, TRIM(UPPER(ais_shiptype)) as ais_shiptype
                                        FROM ",input_schema,".AIS_VL_FSR_VSR_Daily
                                        WHERE vl_fsr_vsr_nomination_number IS NOT NULL)
                                        UNION
                                        (SELECT DISTINCT fsr_nomination_number, TRIM(UPPER(ais_shiptype)) as ais_shiptype
                                        FROM ",input_schema,".m_ais_vl_fsr_ru
                                        WHERE fsr_nomination_number IS NOT NULL)) as md
                                        WHERE t_far_disport_profiler_daily_temp.nomination_number = md.vl_fsr_vsr_nomination_number;
                                        
                                        -- Commodity Cleanup
                                        ALTER TABLE ",input_schema,".far_disport_profiler_daily_temp ADD COLUMN commodity_cleanup VARCHAR(250);
                                        
                                        UPDATE ",input_schema,".far_disport_profiler_daily_temp
                                        SET commodity_cleanup = (CASE 
                                        WHEN UPPER(commodity) LIKE '%IRON%' THEN 'IRON ORE'
                                        WHEN UPPER(commodity) LIKE '%COAL%' THEN 'COAL'
                                        WHEN UPPER(commodity) LIKE '%COPPER%' THEN 'COPPER'
                                        ELSE UPPER(commodity)
                                        END);
                                        
                                        ALTER TABLE ",input_schema,".far_disport_profiler_daily_temp DROP COLUMN commodity;
                                        ALTER TABLE ",input_schema,".far_disport_profiler_daily_temp RENAME COLUMN commodity_cleanup TO commodity;
                                        
                                        INSERT INTO ",input_schema,".far_disport_profiler_daily
                                        SELECT *
                                        FROM ",input_schema,".far_disport_profiler_daily_temp;
                                        "), FALSE)
      end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
      if (is.integer(error)) {
        if (error == -1L){
          status = 'failed'
        }else{
          status = 'succeed'
        }
      }else{
        status = 'succeed'
      }
      sqlQuery(channel, paste0("insert into fa_d2.log_table values ('far_disport_profiler_compile', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
      
      
      # To be run once proper data is received, but the code is setup to get the different disports a vessel goes to
      
      # sqlQuery(channel, paste0("--Creating columns for multiple disports to be run only once proper data for disport ordering is received
      #                   ALTER TABLE ",input_schema,".far_disport_profiler_daily ADD COLUMN disport_1 VARCHAR(250) default NULL;
      #                   ALTER TABLE ",input_schema,".far_disport_profiler_daily ADD COLUMN disport_2 VARCHAR(250) default NULL;
      #                   ALTER TABLE ",input_schema,".far_disport_profiler_daily ADD COLUMN disport_3 VARCHAR(250) default NULL;
      #                   ALTER TABLE ",input_schema,".far_disport_profiler_daily ADD COLUMN disport_4 VARCHAR(250) default NULL;
      #                   ALTER TABLE ",input_schema,".far_disport_profiler_daily ADD COLUMN num_of_disports INT default NULL;
      #                   
      #                   /* Converting long form to wide form to get the number of disports into separate columns, to be run only once proper data for disport ordering is received */
      #                   
      #                   UPDATE ",input_schema,".far_disport_profiler_daily
      #                   SET disport_1 = fd2.discharge_port_desc
      #                   FROM
      #                   (SELECT freight_voyage, discharge_port_desc, ROW_NUMBER() OVER (PARTITION BY freight_voyage) AS rowNo
      #                   FROM ",input_schema,".far_disport_profiler_daily
      #                   GROUP BY freight_voyage, discharge_port_desc
      #                   ORDER BY discharge_port_desc) as fd2
      #                   WHERE t_far_disport_profiler_daily.freight_voyage = fd2.freight_voyage
      #                   AND fd2.rowNo = 1;
      #                   
      #                   UPDATE ",input_schema,".far_disport_profiler_daily
      #                   SET disport_2 = fd2.discharge_port_desc
      #                   FROM
      #                   (SELECT freight_voyage, discharge_port_desc, ROW_NUMBER() OVER (PARTITION BY freight_voyage) AS rowNo
      #                   FROM ",input_schema,".far_disport_profiler_daily
      #                   GROUP BY freight_voyage, discharge_port_desc
      #                   ORDER BY discharge_port_desc) as fd2
      #                   WHERE t_far_disport_profiler_daily.freight_voyage = fd2.freight_voyage
      #                   AND fd2.rowNo = 2;
      #                   
      #                   UPDATE ",input_schema,".far_disport_profiler_daily
      #                   SET disport_3 = fd2.discharge_port_desc
      #                   FROM
      #                   (SELECT freight_voyage, discharge_port_desc, ROW_NUMBER() OVER (PARTITION BY freight_voyage) AS rowNo
      #                   FROM ",input_schema,".far_disport_profiler_daily
      #                   GROUP BY freight_voyage, discharge_port_desc
      #                   ORDER BY discharge_port_desc) as fd2
      #                   WHERE t_far_disport_profiler_daily.freight_voyage = fd2.freight_voyage
      #                   AND fd2.rowNo = 3;
      #                   
      #                   UPDATE ",input_schema,".far_disport_profiler_daily
      #                   SET disport_4 = fd2.discharge_port_desc
      #                   FROM
      #                   (SELECT freight_voyage, discharge_port_desc, ROW_NUMBER() OVER (PARTITION BY freight_voyage) AS rowNo
      #                   FROM ",input_schema,".far_disport_profiler_daily
      #                   GROUP BY freight_voyage, discharge_port_desc
      #                   ORDER BY discharge_port_desc) as fd2
      #                   WHERE t_far_disport_profiler_daily.freight_voyage = fd2.freight_voyage
      #                   AND fd2.rowNo = 4;
      #                   
      #                   /* Additional columns to count the number of disports */
      #                   UPDATE ",input_schema,".far_disport_profiler_daily
      #                   SET num_of_disports = (CASE
      #                                          WHEN disport_1 IS NULL THEN 0
      #                                          WHEN disport_2 IS NULL THEN 1
      #                                          WHEN disport_3 IS NULL THEN 2
      #                                          WHEN disport_4 IS NULL THEN 3
      #                                          WHEN disport_4 IS NOT NULL THEN 4
      #                                          ELSE 0
      #                                          END);"))
      
      # Close the channel
      odbcClose(channel)
    } else {
      print("FAR - Today's Data is NOT available hence no action is performed")
    }
    
    #sys.sleep(10)
    
    #*********************************** Filename: 4. AIS Future State Data Prep 20171016.SQL
    # print("4. AIS Future State Data Prep 20171016.SQL")
    print(paste0("4. AIS Future State Data Prep ",today_date,".SQL"))
    
    
    
    #-- Description: Merges the data from AIS, Vessel List, FSR, VSR. Rules are setup to tag and detag bhp vessels using business logic.
    #-- Dependencies: This should be run after running R code "3. Disport Profiler Calculations.R". Username used to connect to redshift is dapappadmin (hence this user must be given access in order for this to run.)
    #-- Assumptions: NA
    
    #################################### Section 5: Create merge keys for each of the AIS, VL, FSR and VSR daily temp tables
    
    # Channel is opened
    channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
    
    
    # Gets the vessel list data for the current date, add the merge keys, derive commodity and incoterms from port column
    src_data_name <- 'vl_master_daily_data'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("
                                      -- ************************************************Vessel List Daily Table Creation*********************************************
                                      
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".VL_DATA_DAILY_temp;
                                      
                                      -- Get vl data for current date
                                      CREATE TABLE ",input_schema,".VL_DATA_DAILY_temp as 
                                      select DISTINCT *
                                      FROM ",input_schema,".vl_master_daily_data
                                      WHERE vl_selected_date = '",today_date_formatted,"';
                                      
                                      -- Creation of Vessel List Merge Keys, and derive commodity and incoterms from port column
                                      
                                      ALTER TABLE ",input_schema,".VL_DATA_DAILY_temp ADD COLUMN vl_date_key varchar(400); 
                                      ALTER TABLE ",input_schema,".VL_DATA_DAILY_temp ADD COLUMN vl_date_imo_voy_key varchar(400); 
                                      ALTER TABLE ",input_schema,".VL_DATA_DAILY_temp ADD COLUMN vl_commodity varchar(400);
                                      ALTER TABLE ",input_schema,".VL_DATA_DAILY_temp ADD COLUMN vl_incoterms varchar(100);
                                      
                                      UPDATE ",input_schema,".VL_DATA_DAILY_temp
                                      set vl_date_key = concat(VL_Lloyds_Number,VL_Selected_Date)
                                      where not (VL_Lloyds_Number = '0' or VL_Lloyds_Number= ' ' or VL_Lloyds_Number IS NULL);
                                      
                                      UPDATE ",input_schema,".VL_DATA_DAILY_temp
                                      set vl_date_imo_voy_key = VL_Lloyds_Number + VL_Selected_Date + VL_current_voyage_number
                                      where not (VL_Lloyds_Number = '0' or VL_Lloyds_Number= ' ' or VL_Lloyds_Number IS NULL or VL_current_voyage_number = '0' or VL_current_voyage_number= ' ' or VL_current_voyage_number IS NULL) ;
                                      
                                      
                                      UPDATE ",input_schema,".VL_DATA_DAILY_temp
                                      set vl_date_imo_voy_key = VL_Lloyds_Number + VL_Selected_Date + VL_previous_voyage_number
                                      where not (VL_Lloyds_Number = '0' or VL_Lloyds_Number= ' ' or VL_Lloyds_Number IS NULL or VL_current_voyage_number IS NOT NULL or VL_previous_voyage_number IS NULL) ;
                                      
                                      
                                      UPDATE ",input_schema,".VL_DATA_DAILY_temp
                                      set vl_commodity = (CASE
                                      WHEN vl_port = '168' THEN 'Iron Ore'
                                      WHEN vl_port = '144' OR vl_port = '136' OR vl_port = '126' OR vl_port = '94' THEN 'Coal'
                                      WHEN vl_port = '460' OR vl_port = '439' or vl_port = '450' OR vl_port = '456' THEN 'Copper'
                                      ELSE NULL
                                      END);
                                      
                                      UPDATE ",input_schema,".VL_DATA_DAILY_temp
                                      set vl_incoterms = (CASE
                                      WHEN vl_commodity = 'Iron Ore' THEN 'CFR*'
                                      WHEN vl_commodity = 'Coal' THEN 'CFR/CIF*'
                                      WHEN vl_commodity = 'Copper' THEN 'CIF*'
                                      ELSE NULL
                                      END);
                                      
                                      ALTER TABLE ",input_schema,".VL_DATA_DAILY_temp ADD COLUMN vl_last_updated_on_date_temp DATE;
                                      
                                      -- Add last updated on date
                                      UPDATE ",input_schema,".VL_DATA_DAILY_temp
                                      set vl_last_updated_on_date_temp = vl_last_updated_on_date;
                                      
                                      ALTER TABLE ",input_schema,".VL_DATA_DAILY_temp DROP COLUMN vl_last_updated_on_date;
                                      
                                      ALTER TABLE ",input_schema,".VL_DATA_DAILY_temp RENAME COLUMN vl_last_updated_on_date_temp TO vl_last_updated_on_date;
                                      
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('vl_master_daily_data', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    # Gets the FSR data for the current date and add the merge keys
    sqlQuery(channel, paste0("
                             
                             DROP TABLE IF EXISTS ",input_schema,".FSR_Data_Daily_temp;
                             
                             CREATE TABLE ",input_schema,".FSR_Data_Daily_temp AS
                             SELECT DISTINCT *
                             FROM ",input_schema,".fsr_master_daily_data
                             WHERE fsr_selected_date = '",today_date_formatted,"';
                             
                             
                             -- Creation of FSR Merge Keys
                             ALTER TABLE ",input_schema,".FSR_Data_Daily_temp add fsr_date_key varchar(400); 
                             ALTER TABLE ",input_schema,".FSR_Data_Daily_temp add fsr_date_imo_voy_key varchar(400); 
                             
                             
                             
                             UPDATE ",input_schema,".FSR_Data_Daily_temp
                             set fsr_date_key = concat(FSR_Lloyds_Number,FSR_Selected_Date)
                             where not (FSR_Lloyds_Number = '0' or FSR_Lloyds_Number= ' ' or FSR_Lloyds_Number IS NULL) ;
                             
                             UPDATE ",input_schema,".FSR_Data_Daily_temp
                             set fsr_date_imo_voy_key = FSR_Lloyds_Number + FSR_Selected_Date + FSR_freight_voyage_number 
                             where not (FSR_Lloyds_Number = '0' or FSR_Lloyds_Number= ' ' or FSR_Lloyds_Number IS NULL or FSR_freight_voyage_number = '0' or FSR_freight_voyage_number= ' ' or FSR_freight_voyage_number IS NULL) ;
                             
                             
                             ALTER TABLE ",input_schema,".FSR_Data_Daily_temp ADD COLUMN fsr_last_updated_on_date_temp DATE;
                             
                             -- Adding the last updated on date
                             UPDATE ",input_schema,".FSR_Data_Daily_temp
                             set fsr_last_updated_on_date_temp = fsr_last_updated_on_date;
                             
                             ALTER TABLE ",input_schema,".FSR_Data_Daily_temp DROP COLUMN fsr_last_updated_on_date;
                             
                             ALTER TABLE ",input_schema,".FSR_Data_Daily_temp RENAME COLUMN fsr_last_updated_on_date_temp TO fsr_last_updated_on_date;"))
    
    
    # Gets the VSR data for the current date, add the merge keys and add a flag to indicate a record is a final disport
    # Gets the VSR data for the current date, add the merge keys and add a flag to indicate a record is a final disport
    src_data_name <- 'vsr_master_daily_data'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("
                                      DROP TABLE IF EXISTS ",input_schema,".VSR_Data_Daily_temp;
                                      
                                      CREATE TABLE ",input_schema,".VSR_Data_Daily_temp AS
                                      SELECT DISTINCT *
                                      FROM ",input_schema,".vsr_master_daily_data
                                      WHERE vsr_selected_date = '",today_date_formatted,"';
                                      
                                      -- Creation of VSR Merge Keys
                                      ALTER TABLE ",input_schema,".VSR_Data_Daily_temp ADD COLUMN vsr_date_key varchar(400); 
                                      ALTER TABLE ",input_schema,".VSR_Data_Daily_temp ADD COLUMN vsr_date_imo_voy_key varchar(400); 
                                      ALTER TABLE ",input_schema,".VSR_Data_Daily_temp ADD COLUMN vsr_final_disport_flag INTEGER default NULL; 
                                      
                                      
                                      UPDATE ",input_schema,".VSR_Data_Daily_temp
                                      SET vsr_date_key = concat(VSR_Lloyds_Number,VSR_Selected_Date)
                                      WHERE NOT (VSR_Lloyds_Number = '0' or VSR_Lloyds_Number= ' ' or VSR_Lloyds_Number IS NULL) ;
                                      
                                      UPDATE ",input_schema,".VSR_Data_Daily_temp
                                      set vsr_date_imo_voy_key = VSR_Lloyds_Number + VSR_Selected_Date + VSR_voyage_number 
                                      where not (VSR_Lloyds_Number = '0' or VSR_Lloyds_Number= ' ' or VSR_Lloyds_Number IS NULL or VSR_voyage_number = '0' or VSR_voyage_number = ' ' or VSR_voyage_number IS NULL) ;
                                      
                                      
                                      -- Creation of the final disport flag to indicate the final disport row for each selected imo, date, voyage number and nomination key
                                      UPDATE ",input_schema,".VSR_Data_Daily_temp
                                      SET vsr_final_disport_flag = (CASE WHEN rank_flag = 1 THEN 1
                                      ELSE 0
                                      END)
                                      FROM
                                      (SELECT vsr_lloyds_number, vsr_selected_date, vsr_voyage_number, vsr_nomination_key, vsr_departure_date, vsr_contract_actual_quantity, DENSE_RANK() OVER (PARTITION BY vsr_lloyds_number, vsr_selected_date, vsr_voyage_number, vsr_nomination_key 
                                      ORDER BY vsr_departure_date DESC) as rank_flag
                                      FROM ",input_schema,".VSR_Data_Daily_temp
                                      WHERE vsr_departure_date IS NOT NULL
                                      AND NOT (vsr_contract_actual_quantity = 0)) as rank_temp
                                      WHERE VSR_Data_Daily_temp.vsr_selected_date = rank_temp.vsr_selected_date
                                      AND VSR_Data_Daily_temp.vsr_voyage_number = rank_temp.vsr_voyage_number
                                      AND VSR_Data_Daily_temp.vsr_departure_date = rank_temp.vsr_departure_date
                                      AND VSR_Data_Daily_temp.vsr_lloyds_number = rank_temp.vsr_lloyds_number
                                      AND VSR_Data_Daily_temp.vsr_nomination_key = rank_temp.vsr_nomination_key;
                                      
                                      
                                      ALTER TABLE ",input_schema,".VSR_DATA_DAILY_temp ADD COLUMN vsr_last_updated_on_date_temp DATE;
                                      
                                      -- Added last updated on date
                                      UPDATE ",input_schema,".VSR_DATA_DAILY_temp
                                      set vsr_last_updated_on_date_temp = vsr_last_updated_on_date;
                                      
                                      ALTER TABLE ",input_schema,".VSR_DATA_DAILY_temp DROP COLUMN vsr_last_updated_on_date;
                                      
                                      ALTER TABLE ",input_schema,".VSR_DATA_DAILY_temp RENAME COLUMN vsr_last_updated_on_date_temp TO vsr_last_updated_on_date;
                                      
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('vsr_master_daily_data', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    #################################### Section 6: Joining Each days FSR and VSR data
    
    
    
    # Joining of FSR and VSR on date, imo and voyage key.
    src_data_name <- 'FSR_Data_Daily_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("DROP TABLE IF EXISTS ",input_schema,".FSR_VSR_Daily_temp;
                                      
                                      -- Joining FSR and VSR data on date imo and voyage key. 
                                      -- a full join is done to ensure that those without a voyage number are still included in the dataset
                                      CREATE TABLE ",input_schema,".FSR_VSR_Daily_temp as
                                      select * 
                                      FROM ",input_schema,".FSR_Data_Daily_temp x FULL OUTER JOIN ",input_schema,".VSR_Data_Daily_temp as y
                                      on x.FSR_date_imo_voy_key = y.VSR_date_imo_voy_key
                                      WHERE NOT (x.FSR_date_key IS NULL AND y.VSR_date_key IS NULL);
                                      
                                      -- Removing duplicates
                                      DROP TABLE IF EXISTS ",input_schema,".temp_fsr_vsr_rem_dup;
                                      
                                      CREATE TABLE ",input_schema,".temp_fsr_vsr_rem_dup AS
                                      SELECT DISTINCT *
                                      FROM ",input_schema,".FSR_VSR_Daily_temp;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".FSR_VSR_Daily_temp;
                                      
                                      ALTER TABLE ",input_schema,".temp_fsr_vsr_rem_dup
                                      RENAME TO FSR_VSR_Daily_temp;
                                      
                                      
                                      
                                      -- Getting the imo either - fsr or vsr
                                      ALTER TABLE ",input_schema,".FSR_VSR_Daily_temp ADD COLUMN fsr_vsr_imo BIGINT;
                                      ALTER TABLE ",input_schema,".FSR_VSR_Daily_temp ADD COLUMN fsr_vsr_date_position DATE;
                                      ALTER TABLE ",input_schema,".FSR_VSR_Daily_temp ADD COLUMN fsr_vsr_voyage_number BIGINT;
                                      ALTER TABLE ",input_schema,".FSR_VSR_Daily_temp ADD COLUMN fsr_vsr_nomination_number VARCHAR(400);
                                      ALTER TABLE ",input_schema,".FSR_VSR_Daily_temp ADD COLUMN fsr_vsr_imo_date_key VARCHAR(400);
                                      ALTER TABLE ",input_schema,".FSR_VSR_Daily_temp ADD COLUMN fsr_vsr_date_imo_voy_key VARCHAR(1000);
                                      
                                      UPDATE ",input_schema,".FSR_VSR_Daily_temp
                                      SET fsr_vsr_imo = (CASE
                                      WHEN fsr_lloyds_number IS NOT NULL THEN fsr_lloyds_number::BIGINT
                                      WHEN vsr_lloyds_number IS NOT NULL THEN vsr_lloyds_number::BIGINT 
                                      ELSE NULL
                                      END);
                                      
                                      -- Getting the date either - fsr or vsr
                                      UPDATE ",input_schema,".FSR_VSR_Daily_temp
                                      SET fsr_vsr_date_position = (CASE
                                      WHEN fsr_selected_date IS NOT NULL THEN fsr_selected_date
                                      WHEN vsr_selected_date IS NOT NULL THEN vsr_selected_date
                                      ELSE NULL
                                      END);
                                      
                                      
                                      -- Getting the voyage num either - fsr or vsr
                                      UPDATE ",input_schema,".FSR_VSR_Daily_temp
                                      SET fsr_vsr_voyage_number = (CASE
                                      WHEN fsr_freight_voyage_number IS NOT NULL THEN fsr_freight_voyage_number::BIGINT
                                      WHEN vsr_voyage_number IS NOT NULL THEN vsr_voyage_number::BIGINT 
                                      ELSE NULL
                                      END);
                                      
                                      -- Getting the nomination num either - fsr or vsr
                                      UPDATE ",input_schema,".FSR_VSR_Daily_temp
                                      SET fsr_vsr_nomination_number = (CASE
                                      WHEN fsr_nomination_number IS NOT NULL THEN fsr_nomination_number
                                      WHEN vsr_nomination_key IS NOT NULL THEN vsr_nomination_key
                                      ELSE NULL
                                      END);
                                      
                                      
                                      -- Creating the new AIS VL merge keys
                                      UPDATE ",input_schema,".FSR_VSR_Daily_temp
                                      SET fsr_vsr_imo_date_key = concat(fsr_vsr_imo,fsr_vsr_date_position)
                                      WHERE NOT (fsr_vsr_imo IS NULL or fsr_vsr_date_position IS NULL);
                                      
                                      
                                      UPDATE ",input_schema,".FSR_VSR_Daily_temp
                                      SET fsr_vsr_date_imo_voy_key = fsr_vsr_imo::varchar + fsr_vsr_date_position::varchar + fsr_vsr_voyage_number::varchar
                                      WHERE NOT (fsr_vsr_imo IS NULL or fsr_vsr_date_position IS NULL or fsr_vsr_voyage_number IS NULL);
                                      
                                      
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('FSR_VSR_Daily_temp', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    #################################### Section 7: Joining VL on the FSR/VSR temp table (from step 6)
    
    
    
    # Joining VL on the previously joined FSR and VSR data on date imo and voyage key
    src_data_name <- 'FSR_VSR_Daily_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("-- ************************************************Merging of FSR & VSR to VL*********************************************
                                      -- Merging of FSR to VSR on the merge keys (date, imo and voyage number)
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".VL_FSR_VSR_Daily_temp;
                                      
                                      -- Joining VL on the previously joined FSR and VSR data on date imo and voyage key. 
                                      -- a full join is done to ensure that those without a voyage number are still included in the dataset
                                      
                                      CREATE TABLE ",input_schema,".VL_FSR_VSR_Daily_temp as
                                      select * 
                                      FROM ",input_schema,".FSR_VSR_Daily_temp x FULL OUTER JOIN ",input_schema,".VL_Data_Daily_temp as y 
                                      on x.fsr_vsr_date_imo_voy_key = y.vl_date_imo_voy_key
                                      WHERE NOT (x.fsr_vsr_imo_date_key IS NULL AND y.vl_date_key IS NULL);
                                      
                                      -- Removing duplicates
                                      DROP TABLE IF EXISTS ",input_schema,".temp_vl_fsr_vsr_rem_dup;
                                      
                                      CREATE TABLE ",input_schema,".temp_vl_fsr_vsr_rem_dup AS
                                      SELECT DISTINCT *
                                      FROM ",input_schema,".VL_FSR_VSR_Daily_temp;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".VL_FSR_VSR_Daily_temp;
                                      
                                      ALTER TABLE ",input_schema,".temp_vl_fsr_vsr_rem_dup
                                      RENAME TO VL_FSR_VSR_Daily_temp;
                                      
                                      
                                      
                                      
                                      ALTER TABLE ",input_schema,".VL_FSR_VSR_Daily_temp ADD COLUMN vl_fsr_vsr_imo BIGINT;
                                      ALTER TABLE ",input_schema,".VL_FSR_VSR_Daily_temp ADD COLUMN vl_fsr_vsr_date_position DATE;
                                      ALTER TABLE ",input_schema,".VL_FSR_VSR_Daily_temp ADD COLUMN vl_fsr_vsr_voyage_number BIGINT;
                                      ALTER TABLE ",input_schema,".VL_FSR_VSR_Daily_temp ADD COLUMN vl_fsr_vsr_nomination_number VARCHAR(400);
                                      ALTER TABLE ",input_schema,".VL_FSR_VSR_Daily_temp ADD COLUMN vl_fsr_vsr_imo_date_key VARCHAR(400);
                                      
                                      -- Getting the imo either - vl or fsr/vsr
                                      UPDATE ",input_schema,".VL_FSR_VSR_Daily_temp
                                      SET vl_fsr_vsr_imo = (CASE
                                      WHEN fsr_vsr_imo IS NOT NULL THEN fsr_vsr_imo::BIGINT
                                      WHEN vl_lloyds_number IS NOT NULL THEN vl_lloyds_number::BIGINT 
                                      ELSE NULL
                                      END);
                                      
                                      -- Getting the date either - vl or fsr/vsr
                                      UPDATE ",input_schema,".VL_FSR_VSR_Daily_temp
                                      SET vl_fsr_vsr_date_position = (CASE
                                      WHEN fsr_vsr_date_position IS NOT NULL THEN fsr_vsr_date_position
                                      WHEN vl_selected_date IS NOT NULL THEN vl_selected_date
                                      ELSE NULL
                                      END);
                                      
                                      
                                      -- Getting the voyage num either - vl or fsr/vsr
                                      UPDATE ",input_schema,".VL_FSR_VSR_Daily_temp
                                      SET vl_fsr_vsr_voyage_number = (CASE
                                      WHEN fsr_vsr_voyage_number IS NOT NULL THEN fsr_vsr_voyage_number::BIGINT 
                                      WHEN vl_current_voyage_number IS NOT NULL THEN vl_current_voyage_number::BIGINT 
                                      WHEN vl_previous_voyage_number IS NOT NULL THEN vl_previous_voyage_number::BIGINT
                                      ELSE NULL
                                      END);
                                      
                                      -- Getting the nomination num either - vl or fsr/vsr
                                      UPDATE ",input_schema,".VL_FSR_VSR_Daily_temp
                                      SET vl_fsr_vsr_nomination_number = (CASE
                                      WHEN fsr_vsr_nomination_number IS NOT NULL THEN fsr_vsr_nomination_number
                                      WHEN vl_handedover_nominations IS NOT NULL THEN vl_handedover_nominations 
                                      ELSE NULL
                                      END);
                                      
                                      
                                      -- Creating the new AIS VL FSR VSR merge keys
                                      UPDATE ",input_schema,".VL_FSR_VSR_Daily_temp
                                      SET vl_fsr_vsr_imo_date_key = concat(vl_fsr_vsr_imo,vl_fsr_vsr_date_position)
                                      WHERE NOT (vl_fsr_vsr_imo IS NULL or vl_fsr_vsr_date_position IS NULL);
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('VL_FSR_VSR_Daily_temp', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    
    #################################### Section 8: Joining AIS on the VL/FSR/VSR temp table (from step 7
    
    
    
    
    # Joining AIS on the previously joined VL, FSR and VSR data on date imo and voy key. 
    src_data_name <- 'AIS_VL_FSR_VSR_Daily_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("DROP TABLE IF EXISTS ",input_schema,".AIS_VL_FSR_VSR_Daily_temp;
                                      
                                      -- Joining AIS on the previously joined VL, FSR and VSR data on date and imo key. 
                                      CREATE TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp as
                                      select * 
                                      FROM ",input_schema,".ais_data_daily_temp x FULL OUTER JOIN ",input_schema,".VL_FSR_VSR_Daily_temp as y
                                      on x.AIS_date_key = y.vl_fsr_vsr_imo_date_key
                                      WHERE NOT (x.AIS_date_key IS NULL AND y.vl_fsr_vsr_imo_date_key IS NULL);
                                      
                                      
                                      -- Removing duplicates
                                      DROP TABLE IF EXISTS ",input_schema,".temp_ais_vl_fsr_vsr_rem_dup;
                                      
                                      CREATE TABLE ",input_schema,".temp_ais_vl_fsr_vsr_rem_dup AS
                                      SELECT DISTINCT *
                                      FROM ",input_schema,".AIS_VL_FSR_VSR_Daily_temp;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".AIS_VL_FSR_VSR_Daily_temp;
                                      
                                      ALTER TABLE ",input_schema,".temp_ais_vl_fsr_vsr_rem_dup
                                      RENAME TO AIS_VL_FSR_VSR_Daily_temp;
                                      
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN ais_vl_fsr_vsr_imo BIGINT;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN ais_vl_fsr_vsr_date_position DATE;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN ais_vl_fsr_vsr_imo_date_key VARCHAR(1000);
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN ais_vl_fsr_vsr_incoterms VARCHAR(400);
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN ais_vl_fsr_vsr_commodity VARCHAR(400);
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN ais_vl_fsr_vsr_vessel_name VARCHAR(400);
                                      
                                      -- Getting the imo either - ais, vl, fsr, vsr
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET ais_vl_fsr_vsr_imo = (CASE
                                      WHEN ais_static_imo IS NOT NULL THEN ais_static_imo
                                      WHEN vl_fsr_vsr_imo IS NOT NULL THEN vl_fsr_vsr_imo
                                      ELSE NULL
                                      END);
                                      
                                      -- Getting the date either - ais, vl, fsr, vsr
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET ais_vl_fsr_vsr_date_position = (CASE
                                      WHEN ais_date_position IS NOT NULL THEN ais_date_position
                                      WHEN vl_fsr_vsr_date_position IS NOT NULL THEN vl_fsr_vsr_date_position
                                      ELSE NULL
                                      END);
                                      
                                      -- Creation of merge keys                            
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET ais_vl_fsr_vsr_imo_date_key = concat(ais_vl_fsr_vsr_imo,ais_vl_fsr_vsr_date_position)
                                      WHERE NOT (ais_vl_fsr_vsr_imo IS NULL or ais_vl_fsr_vsr_date_position IS NULL);
                                      
                                      
                                      -- Getting the incoterms either - fsr or vl
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET ais_vl_fsr_vsr_incoterms = (CASE
                                      WHEN fsr_incoterms_1 IS NOT NULL THEN fsr_incoterms_1
                                      WHEN vl_incoterms IS NOT NULL THEN vl_incoterms
                                      ELSE NULL
                                      END);
                                      
                                      -- Getting the incoterms either - fsr or vl
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET ais_vl_fsr_vsr_commodity = (CASE
                                      WHEN fsr_commodity IS NOT NULL THEN fsr_commodity
                                      WHEN vl_commodity IS NOT NULL THEN vl_commodity
                                      ELSE NULL
                                      END);
                                      
                                      -- Getting the vessel name either - fsr or vsr
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET ais_vl_fsr_vsr_vessel_name = (CASE
                                      WHEN ais_static_name IS NOT NULL THEN UPPER(ais_static_name)
                                      WHEN vl_vessel_name IS NOT NULL THEN UPPER(vl_vessel_name)
                                      WHEN fsr_vessel_extension_identifier IS NOT NULL THEN UPPER(fsr_vessel_extension_identifier)
                                      WHEN vsr_vessel IS NOT NULL THEN UPPER(vsr_vessel)
                                      ELSE NULL
                                      END);    
                                      
                                      
                                      -- Removing duplicates
                                      DROP TABLE IF EXISTS ",input_schema,".temp_main;
                                      
                                      CREATE TABLE ",input_schema,".temp_main AS
                                      SELECT DISTINCT *
                                      FROM ",input_schema,".AIS_VL_FSR_VSR_Daily_temp;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".AIS_VL_FSR_VSR_Daily_temp;
                                      
                                      ALTER TABLE ",input_schema,".temp_main RENAME TO AIS_VL_FSR_VSR_Daily_temp;
                                      
                                      
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('ais_vl_fsr_vsr_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    
    #################################### Section 9: Creation of Flags to identify the data sources
    
    
    # Create a flag field for each of the four(4) data sources in the now merged table (from step 8) - and indicate whether the source is available for the most recent date
    src_data_name <- 'AIS_VL_FSR_VSR_Daily_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("-- ************************************************Creation of AIS, Vessel List, FSR and VSR flags*********************************************
                                      -- Each of the AIS Vessel List, FSR and VSR flags, indicate if the record in AIS has appeared in their respective tables
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN ais_flag INTEGER;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN vl_flag INTEGER;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN fsr_flag INTEGER;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN vsr_flag INTEGER;
                                      
                                      
                                      -- Flag that checks if vessel has appeared on AIS for that selected date
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET ais_flag = (CASE
                                      WHEN ais_date_position IS NOT NULL THEN 1
                                      ELSE 0
                                      END);
                                      
                                      -- Flag that checks if vessel has appeared on the vessel list for that selected date
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET vl_flag = (CASE
                                      WHEN vl_selected_date IS NOT NULL THEN 1
                                      ELSE 0
                                      END);
                                      
                                      -- Flag that checks if vessel has appeared on the FSR for each selected date
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET fsr_flag = (CASE
                                      WHEN fsr_selected_date IS NOT NULL THEN 1
                                      ELSE 0
                                      END);
                                      
                                      -- Flag that checks if vessel has appeared on the VSR for each selected date
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET vsr_flag = (CASE
                                      WHEN vsr_selected_date IS NOT NULL THEN 1
                                      ELSE 0
                                      END);
                                      
                                      -- An additional flag is added to check cases where the vessel has appeared in AIS but has not appeared in Vessel List, FSR and VSR tables
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN vl_fsr_vsr_null_flag INTEGER;
                                      
                                      -- Flag that checks if vessel has not appeared on the vessel list, FSR and VSR for each selected date
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET vl_fsr_vsr_null_flag = (CASE
                                      WHEN (vl_selected_date IS NULL AND fsr_selected_date IS NULL AND vsr_selected_date IS NULL) THEN 1
                                      ELSE 0
                                      END);"), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('ais_vl_fsr_vsr_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    #################################### Section 10: Tagging and De-tagging of BHP Vessels 
    
    
    
    # Tagging and Detagging Logic
    # Tagging and Detagging Logic
    src_data_name <- 'AIS_VL_FSR_VSR_Daily_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("-- **********************************************Tagging and Detagging Logic for BHP***********************************************
                                      -- Tagging Logic: (it is tagged if it follows the following rules)
                                      -- Rule 1: If the vessel is in the vessel list
                                      -- Rule 2: If incoterms is FOB and fsr_eta_date is 20 days before the fsr_selected_date
                                      -- Rule 3: If incoterms is CFR or CIF
                                      -- Rule 4: If the vessel is not available in vessel list, fsr and vsr and if the previous date for that imo says it is tagged, then it is tagged
                                      
                                      -- Detagging Logic: (it is detagged if it follows the following rules)
                                      -- Rule 1: If vessel is in the VSR and departure date < selected date and departure date version is A (actual) and it is the final disport
                                      -- Rule 2: If vessel is in FSR and if incoterms is FOB and BOL date is less than 10 days ago from selected date
                                      
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN bhp_tagging INTEGER;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN bhp_de_tagging INTEGER;
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET bhp_tagging = (CASE
                                      WHEN vl_flag = 1 THEN 1
                                      WHEN ais_vl_fsr_vsr_incoterms = 'CFR' OR ais_vl_fsr_vsr_incoterms = 'CIF' or ais_vl_fsr_vsr_incoterms = 'CFR*'
                                      OR ais_vl_fsr_vsr_incoterms = 'CIF*' or ais_vl_fsr_vsr_incoterms = 'CFR/CIF*' THEN 1
                                      WHEN fsr_flag = 1 AND ais_vl_fsr_vsr_incoterms = 'FOB' AND fsr_eta_date <= DATEADD(day, 20, fsr_selected_date) THEN 1
                                      ELSE 0
                                      END);
                                      
                                      
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET bhp_de_tagging = (CASE
                                      WHEN vsr_flag = 1 AND VSR_Departure_Date < vsr_selected_date AND vsr_departure_date_version = 'A' AND vsr_final_disport_flag = 1 THEN 1 
                                      WHEN fsr_flag = 1 AND ais_vl_fsr_vsr_incoterms = 'FOB' AND fsr_bol_date < DATEADD(day, -10, fsr_selected_date) THEN 1
                                      ELSE 0
                                      END);"), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('ais_vl_fsr_vsr_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    src_data_name <- 'AIS_VL_FSR_VSR_Daily_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("-- **********************************************Cases to be disregarded while tagging and detagging***********************************************
                                      -- Rules where tagging and detagging must be disregarded and is not application
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN tag_detag_disregard INTEGER;
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET tag_detag_disregard = (CASE
                                      WHEN vsr_flag = 1 AND NOT (vsr_final_disport_flag = 1) THEN 1
                                      ELSE 0
                                      END);
                                      
                                      
                                      
                                      -- flag to indicate whether vsr departure date is null
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN vsr_departure_date_null_flag INTEGER;
                                      
                                      --- this is used for ranking
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET vsr_departure_date_null_flag = (CASE
                                      WHEN VSR_Departure_Date IS NULL THEN 1
                                      ELSE 0
                                      END);
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN vl_fsr_vsr_voyage_number_raw BIGINT;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp ADD COLUMN vl_fsr_vsr_nomination_number_raw VARCHAR(400);
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET vl_fsr_vsr_voyage_number_raw = vl_fsr_vsr_voyage_number;
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
                                      SET vl_fsr_vsr_nomination_number_raw = vl_fsr_vsr_nomination_number;
                                      
                                      
                                      INSERT INTO ",input_schema,".AIS_VL_FSR_VSR_Daily
                                      SELECT *
                                      FROM ",input_schema,".AIS_VL_FSR_VSR_Daily_temp;"), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('ais_vl_fsr_vsr_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    #Detagging Cleanup Logic - part 1
    
    print ("Detagging cleanup logic")
    
    src_data_name <- 'AIS_VL_FSR_VSR_Daily'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("-- *************************************************************** CFR
                                      -- list of voyage number, date combinations to find the first bol or vessel eta date
                                      DROP TABLE IF EXISTS ",input_schema,".temp_voy_tag;
                                      CREATE TABLE ",input_schema,".temp_voy_tag AS
                                      WITH temp_voy AS
                                      (SELECT DISTINCT vl_fsr_vsr_voyage_number, ais_vl_fsr_vsr_date_position, fsr_bol_date, TRUNC(vl_vessel_eta) as vl_vessel_eta, 
                                      (CASE 
                                      WHEN UPPER(ais_vl_fsr_vsr_incoterms) LIKE '%CFR%' THEN 'CFR' 
                                      WHEN UPPER(ais_vl_fsr_vsr_incoterms) LIKE '%CIF%' THEN 'CFR'
                                      WHEN UPPER(ais_vl_fsr_vsr_incoterms) LIKE '%FOB%' THEN 'FOB'
                                      ELSE NULL
                                      END) as incoterms,
                                      (CASE 
                                      WHEN UPPER(ais_vl_fsr_vsr_commodity) LIKE '%IRON%' THEN 'IRON ORE' 
                                      WHEN UPPER(ais_vl_fsr_vsr_commodity) LIKE '%COAL%' THEN 'COAL'
                                      WHEN UPPER(ais_vl_fsr_vsr_commodity) LIKE '%COPPER%' THEN 'COPPER'
                                      ELSE 'OTHER'
                                      END) as commodity
                                      FROM ",input_schema,".AIS_VL_FSR_VSR_Daily
                                      WHERE vl_fsr_vsr_voyage_number IN (SELECT DISTINCT vl_fsr_vsr_voyage_number
                                      FROM ",input_schema,".AIS_VL_FSR_VSR_Daily
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      --AND bhp_tagging = 1
                                      --AND bhp_de_tagging = 0
                                      AND vl_fsr_vsr_voyage_number IS NOT NULL)
                                      ORDER BY vl_fsr_vsr_voyage_number, ais_vl_fsr_vsr_date_position)
                                      SELECT *
                                      FROM temp_voy
                                      WHERE incoterms = 'CFR';
                                      
                                      
                                      -- Get the first bol date for each of the voyage numbers
                                      DROP TABLE IF EXISTS ",input_schema,".cfr_detagging_cleanup;
                                      
                                      CREATE TABLE ",input_schema,".cfr_detagging_cleanup AS
                                      SELECT vl_fsr_vsr_voyage_number as voyage_number, fsr_bol_date as bol_or_eta_date, incoterms, commodity
                                      FROM
                                      (WITH temp_voy_bol AS
                                      (SELECT DISTINCT vl_fsr_vsr_voyage_number, incoterms, commodity, ais_vl_fsr_vsr_date_position, fsr_bol_date, 
                                      DENSE_RANK() OVER (PARTITION BY vl_fsr_vsr_voyage_number
                                      ORDER BY vl_fsr_vsr_voyage_number, ais_vl_fsr_vsr_date_position) as rank
                                      FROM ",input_schema,".temp_voy_tag
                                      WHERE fsr_bol_date IS NOT NULL
                                      AND incoterms = 'CFR'
                                      ORDER BY vl_fsr_vsr_voyage_number, ais_vl_fsr_vsr_date_position)
                                      SELECT DISTINCT vl_fsr_vsr_voyage_number, fsr_bol_date, incoterms, commodity
                                      FROM temp_voy_bol
                                      WHERE rank = 1
                                      ORDER BY vl_fsr_vsr_voyage_number, ais_vl_fsr_vsr_date_position)
                                      UNION
                                      (WITH temp_voy_vl_eta AS
                                      (SELECT DISTINCT vl_fsr_vsr_voyage_number, incoterms, commodity, ais_vl_fsr_vsr_date_position, vl_vessel_eta, 
                                      DENSE_RANK() OVER (PARTITION BY vl_fsr_vsr_voyage_number
                                      ORDER BY vl_fsr_vsr_voyage_number, ais_vl_fsr_vsr_date_position) as rank
                                      FROM ",input_schema,".temp_voy_tag
                                      WHERE vl_vessel_eta IS NOT NULL
                                      AND vl_fsr_vsr_voyage_number NOT IN (SELECT DISTINCT vl_fsr_vsr_voyage_number
                                      FROM ",input_schema,".temp_voy_tag
                                      WHERE fsr_bol_date IS NOT NULL)
                                      AND incoterms = 'CFR'
                                      ORDER BY vl_fsr_vsr_voyage_number, ais_vl_fsr_vsr_date_position)
                                      SELECT DISTINCT vl_fsr_vsr_voyage_number, vl_vessel_eta, incoterms, commodity
                                      FROM temp_voy_vl_eta
                                      WHERE rank = 1
                                      ORDER BY vl_fsr_vsr_voyage_number, ais_vl_fsr_vsr_date_position);
                                      
                                      -- ***************************************************** FOB
                                      
                                      
                                      
                                      -- list of nomination number, date combinations to find the first date
                                      DROP TABLE IF EXISTS ",input_schema,".temp_nom_tag;
                                      CREATE TABLE ",input_schema,".temp_nom_tag AS
                                      WITH temp_nom AS
                                      (SELECT DISTINCT vl_fsr_vsr_nomination_number,
                                      (CASE 
                                      WHEN UPPER(ais_vl_fsr_vsr_incoterms) LIKE '%CFR%' THEN 'CFR' 
                                      WHEN UPPER(ais_vl_fsr_vsr_incoterms) LIKE '%CIF%' THEN 'CFR'
                                      WHEN UPPER(ais_vl_fsr_vsr_incoterms) LIKE '%FOB%' THEN 'FOB'
                                      ELSE NULL
                                      END) as incoterms,
                                      (CASE 
                                      WHEN UPPER(ais_vl_fsr_vsr_commodity) LIKE '%IRON%' THEN 'IRON ORE' 
                                      WHEN UPPER(ais_vl_fsr_vsr_commodity) LIKE '%COAL%' THEN 'COAL'
                                      WHEN UPPER(ais_vl_fsr_vsr_commodity) LIKE '%COPPER%' THEN 'COPPER'
                                      ELSE 'OTHER'
                                      END) as commodity
                                      FROM ",input_schema,".AIS_VL_FSR_VSR_Daily
                                      ORDER BY vl_fsr_vsr_nomination_number)
                                      SELECT *
                                      FROM temp_nom
                                      WHERE incoterms = 'FOB'
                                      AND vl_fsr_vsr_nomination_number IS NOT NULL
                                      AND vl_fsr_vsr_nomination_number IN (SELECT DISTINCT vl_fsr_vsr_nomination_number
                                      FROM ",input_schema,".AIS_VL_FSR_VSR_Daily
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      --AND bhp_tagging = 1
                                      --AND bhp_de_tagging = 0
                                      AND vl_fsr_vsr_nomination_number IS NOT NULL)
                                      ORDER BY vl_fsr_vsr_nomination_number;
                                      
                                      -- Get the first available date for each of the nomination numbers
                                      DROP TABLE IF EXISTS ",input_schema,".fob_detagging_cleanup;
                                      
                                      CREATE TABLE ",input_schema,".fob_detagging_cleanup AS
                                      WITH temp_nom_min_date AS
                                      (SELECT DISTINCT vl_fsr_vsr_nomination_number, MIN(ais_vl_fsr_vsr_date_position) as nom_no_first_date
                                      FROM ",input_schema,".AIS_VL_FSR_VSR_Daily
                                      WHERE ais_vl_fsr_vsr_incoterms = 'FOB'
                                      AND vl_fsr_vsr_nomination_number IS NOT NULL
                                      GROUP BY vl_fsr_vsr_nomination_number
                                      ORDER BY vl_fsr_vsr_nomination_number)
                                      SELECT x.*, y.nom_no_first_date
                                      FROM ",input_schema,".temp_nom_tag as x LEFT JOIN temp_nom_min_date as y
                                      ON x.vl_fsr_vsr_nomination_number = y.vl_fsr_vsr_nomination_number;
                                      
                                      
                                      ----- Updating the table to apply the detagging cleanup (need to key in the date)
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily
                                      SET bhp_de_tagging = 2
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND vl_fsr_vsr_voyage_number IN (WITH temp_cfr_detag AS
                                      (SELECT x.*, y.total_time, ('",today_date_formatted,"' - x.bol_or_eta_date) as date_change
                                      FROM ",input_schema,".cfr_detagging_cleanup as x LEFT JOIN ",input_schema,".detagging_cutoff_thresholds as y
                                      ON x.incoterms = y.incoterms AND x.commodity = y.commodity
                                      WHERE ('",today_date_formatted,"' - x.bol_or_eta_date) > y.total_time)
                                      SELECT DISTINCT voyage_number
                                      FROM temp_cfr_detag);
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily
                                      SET bhp_de_tagging = 2
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND vl_fsr_vsr_nomination_number IN (WITH temp_fob_detag AS
                                      (SELECT x.*, y.total_time, ('",today_date_formatted,"' - x.nom_no_first_date) as date_change
                                      FROM ",input_schema,".fob_detagging_cleanup as x LEFT JOIN ",input_schema,".detagging_cutoff_thresholds as y
                                      ON x.incoterms = y.incoterms AND x.commodity = y.commodity
                                      WHERE ('",today_date_formatted,"' - x.nom_no_first_date) > y.total_time)
                                      SELECT DISTINCT vl_fsr_vsr_nomination_number
                                      FROM temp_fob_detag);
                                      
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".temp_nom_tag;
                                      DROP TABLE IF EXISTS ",input_schema,".temp_voy_tag;
                                      DROP TABLE IF EXISTS ",input_schema,".cfr_detagging_cleanup;
                                      DROP TABLE IF EXISTS ",input_schema,".fob_detagging_cleanup;
                                      
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('ais_vl_fsr_vsr_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    src_data_name <- 'AIS_VL_FSR_VSR_Daily'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("
                                      DROP TABLE IF EXISTS ",input_schema,".imo_date_list;
                                      
                                      CREATE TABLE ",input_schema,".imo_date_list AS
                                      WITH temp_ranking AS 
                                      (SELECT DISTINCT ais_vl_fsr_vsr_imo , ais_vl_fsr_vsr_date_position , 
                                      DENSE_RANK() OVER (PARTITION BY ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position
                                      ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_date_position ASC, VSR_Departure_Date DESC, 
                                      vl_fsr_vsr_voyage_number ASC, vl_current_voyage_number ASC, fsr_eta_date DESC, vsr_final_disport_flag ASC) as rank                
                                      FROM ",input_schema,".AIS_VL_FSR_VSR_Daily
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_date_position ASC, vsr_departure_date_null_flag DESC, vl_fsr_vsr_voyage_number DESC, vl_current_voyage_number ASC, fsr_eta_date DESC, vsr_final_disport_flag ASC
                                      ),
                                      
                                      temp_imo_date_ranking AS
                                      (SELECT ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position, COUNT(rank) as count_rank
                                      FROM temp_ranking
                                      GROUP BY ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position
                                      ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_date_position ASC)
                                      
                                      SELECT CONCAT(ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position) as imo_date_concat
                                      FROM temp_imo_date_ranking
                                      WHERE count_rank = 1;"),FALSE)
    
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('historical_imo_list', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'SAFETY_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    #   sqlQuery(channel, paste0("DROP TABLE IF EXISTS ",input_schema,".imo_date_list;
    #                            
    #                            CREATE TABLE ",input_schema,".imo_date_list AS
    #                            WITH count_all AS
    #                            (SELECT DISTINCT ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position,
    #                            DENSE_RANK() OVER (PARTITION BY ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position
    #                            ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_date_position ASC, VSR_Departure_Date DESC, vl_fsr_vsr_voyage_number ASC, vl_current_voyage_number ASC, fsr_eta_date DESC, vsr_final_disport_flag ASC) as rank              
    #                            FROM ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
    #                            WHERE tag_detag_disregard = 0
    #                            ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_date_position ASC, VSR_Departure_Date DESC, vl_fsr_vsr_voyage_number ASC, 
    #                            vl_current_voyage_number ASC, fsr_eta_date DESC, vsr_final_disport_flag ASC),
    #                            
    #                            count_all_rank AS
    #                            (SELECT ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position, COUNT(rank) as count_rank
    #                            FROM count_all
    #                            GROUP BY ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position
    #                            ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_date_position ASC),
    #                            
    #                            count_vsr_null AS
    #                            (SELECT DISTINCT ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position, 
    #                            DENSE_RANK() OVER (PARTITION BY ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position
    #                            ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_date_position ASC, VSR_Departure_Date DESC, vl_fsr_vsr_voyage_number ASC, vl_current_voyage_number ASC, fsr_eta_date DESC, vsr_final_disport_flag ASC) as rank             
    #                            FROM ",input_schema,".AIS_VL_FSR_VSR_Daily_temp
    #                            WHERE tag_detag_disregard = 0
    #                            AND (vsr_departure_date IS NULL AND vl_fsr_vsr_voyage_number IS NULL)
    #                            AND ((vsr_departure_date_version = 'Not assigned') OR vsr_departure_date_version IS NULL)
    #                            ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_date_position ASC, VSR_Departure_Date DESC, vl_fsr_vsr_voyage_number ASC, 
    #                            vl_current_voyage_number ASC, fsr_eta_date DESC, vsr_final_disport_flag ASC),
    #                            
    #                            count_vsr_null_rank AS
    #                            (SELECT ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position, COUNT(rank) as count_rank
    #                            FROM count_vsr_null
    #                            GROUP BY ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position
    #                            ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_date_position ASC)
    #                            
    #                            SELECT DISTINCT CONCAT(x.ais_vl_fsr_vsr_imo, y.ais_vl_fsr_vsr_date_position) as imo_date_concat
    #                            FROM count_all_rank as x, count_vsr_null_rank as y
    #                            WHERE x.ais_vl_fsr_vsr_date_position = y.ais_vl_fsr_vsr_date_position
    #                            AND x.ais_vl_fsr_vsr_date_position = y.ais_vl_fsr_vsr_date_position
    #                            AND x.count_rank = y.count_rank
    #                            ORDER BY x.ais_vl_fsr_vsr_imo ASC, x.ais_vl_fsr_vsr_date_position ASC;
    #                            
    #                            "))
    
    
    
    src_data_name <- 'AIS_VL_FSR_VSR_Daily'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp;
                                      
                                      -- Sort and rank records, keeping only the first record of each IMO, date combination
                                      CREATE TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp AS
                                      WITH temp_dep_voy_rank AS 
                                      (SELECT *
                                      FROM
                                      
                                      (SELECT DISTINCT *, DENSE_RANK() OVER (PARTITION BY ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position
                                      ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_date_position ASC, VSR_Departure_Date DESC, vl_fsr_vsr_voyage_number ASC, 
                                      vl_current_voyage_number ASC, fsr_eta_date DESC, vsr_final_disport_flag ASC) as vsr_departure_voy_rank                
                                      FROM ",input_schema,".AIS_VL_FSR_VSR_Daily
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND tag_detag_disregard = 0 
                                      AND NOT (vsr_departure_date IS NULL AND vl_fsr_vsr_voyage_number IS NULL)
                                      AND (NOT (vsr_departure_date_version = 'Not assigned') OR vsr_departure_date_version IS NULL))
                                      
                                      UNION
                                      
                                      (SELECT DISTINCT *, DENSE_RANK() OVER (PARTITION BY ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position
                                      ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_date_position ASC, VSR_Departure_Date DESC, vl_fsr_vsr_voyage_number ASC, 
                                      vl_current_voyage_number ASC, fsr_eta_date DESC, vsr_final_disport_flag ASC) as vsr_departure_voy_rank                
                                      FROM ",input_schema,".AIS_VL_FSR_VSR_Daily
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND CONCAT(ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position) IN (SELECT DISTINCT imo_date_concat
                                      FROM ",input_schema,".imo_date_list))
                                      ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_date_position ASC, vsr_departure_date_null_flag DESC, vl_fsr_vsr_voyage_number DESC, vl_current_voyage_number ASC)
                                      SELECT *
                                      FROM temp_dep_voy_rank
                                      WHERE vsr_departure_voy_rank = 1;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".imo_date_list;
                                      
                                      -- ********************************************************************************************
                                      -- If a vessel has been tagged on the previous day, and it drops off due to data inconsistencies, we need to still keep it tagged.
                                      
                                      -- Creation of a 2 columns
                                      -- 1: gets the previous date for eah row for each imo
                                      -- 2: gets the difference of the current date and previous date for each row (to check for gaps in the data)
                                      
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".temp_spotfire_daily;
                                      CREATE TABLE ",input_schema,".temp_spotfire_daily AS
                                      WITH lag_date as 
                                      (SELECT lag(ais_vl_fsr_vsr_date_position,1) over (partition by ais_vl_fsr_vsr_imo 
                                      order by ais_vl_fsr_vsr_date_position) as ais_prev_date_position , *
                                      FROM ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      ORDER BY ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position)
                                      SELECT DATEDIFF(d, ais_prev_date_position, ais_vl_fsr_vsr_date_position) as ais_date_difference, *
                                      FROM lag_date;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp;
                                      
                                      ALTER TABLE ",input_schema,".temp_spotfire_daily RENAME TO AIS_VL_FSR_VSR_Daily_Spotfire_temp;
                                      
                                      -- **********************************************Combining the Tagging and Detagging to find the tagged BHP vessels***********************************************
                                      -- If a vessel's tagged is 1 and detag is 0, then it is tagged
                                      -- Exception: If vessel list, fsr and vsr are null for that period, then we check the difference of the current date and previous date, if that is greater than 2 it is marked as detagged
                                      
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN flag_tag_detag_combine INTEGER;
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET flag_tag_detag_combine = (CASE 
                                      WHEN bhp_de_tagging = 2 THEN 2
                                      WHEN bhp_de_tagging = 1 THEN 2
                                      WHEN vl_fsr_vsr_null_flag = 1 THEN (CASE 
                                      WHEN ais_date_difference > 2 THEN 0 
                                      ELSE NULL
                                      END)
                                      WHEN bhp_tagging = 1 AND bhp_de_tagging = 0 THEN 1 -- 1 is tagged
                                      ELSE 0
                                      END);
                                      
                                      
                                      
                                      -- **********************************************The tagged vessels are segregated by commodity***********************************************
                                      -- For the vessels that are tagged, it will check for their commodity and assign accordingly
                                      
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN flag_bhp VARCHAR(250);
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET flag_bhp = (CASE 
                                      WHEN vl_fsr_vsr_null_flag = 1 THEN (CASE 
                                      WHEN ais_date_difference > 2 THEN 'Non-BHP' 
                                      ELSE NULL
                                      END)
                                      WHEN flag_tag_detag_combine = 1 AND UPPER(ais_vl_fsr_vsr_commodity) LIKE '%IRON%' THEN 'BHP-Iron Ore'
                                      WHEN flag_tag_detag_combine = 1 AND UPPER(ais_vl_fsr_vsr_commodity) LIKE '%COAL%' THEN 'BHP-Coal'
                                      WHEN flag_tag_detag_combine = 1 AND UPPER(ais_vl_fsr_vsr_commodity) LIKE '%COPPER%' THEN 'BHP-Copper'
                                      WHEN flag_tag_detag_combine = 1 THEN 'BHP-Others'
                                      ELSE 'Non-BHP'
                                      END);
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('ais_vl_fsr_vsr_daily_spotfire', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    #################################### Section 11: Data Smoothing
    
    
    
    # Where tagged IMO, date* data is missing in the most recent day, drop-down information from the previously available data (for each respective IMO, date*). 
    # Here we prep the data to be dropped down
    src_data_name <- 'AIS_VL_FSR_VSR_Daily_Spotfire_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("-- Data smoothing is performed here, so we put placeholders to stop from pulling down from previous when it has been detagged, the rest will be dropped down
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN ais_vl_fsr_vsr_commodity_missing_gaps VARCHAR(250);
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN ais_vl_fsr_vsr_incoterms_missing_gaps VARCHAR(250);
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET ais_vl_fsr_vsr_commodity_missing_gaps = (CASE
                                      WHEN vl_fsr_vsr_null_flag = 1 THEN (CASE 
                                      WHEN ais_date_difference > 2 THEN 'NOT APPLICABLE' 
                                      ELSE NULL
                                      END)
                                      ELSE ais_vl_fsr_vsr_commodity
                                      END);
                                      
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET ais_vl_fsr_vsr_incoterms_missing_gaps = (CASE 
                                      WHEN vl_fsr_vsr_null_flag = 1 THEN (CASE 
                                      WHEN ais_date_difference > 2 THEN 'NOT APPLICABLE' 
                                      ELSE NULL
                                      END)
                                      ELSE ais_vl_fsr_vsr_incoterms
                                      END);
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp DROP COLUMN ais_vl_fsr_vsr_commodity;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp DROP COLUMN ais_vl_fsr_vsr_incoterms;
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp RENAME COLUMN ais_vl_fsr_vsr_commodity_missing_gaps TO ais_vl_fsr_vsr_commodity;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp RENAME COLUMN ais_vl_fsr_vsr_incoterms_missing_gaps TO ais_vl_fsr_vsr_incoterms;
                                      
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN vl_fsr_vsr_voyage_number_missing_gaps bigint;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN vl_fsr_vsr_nomination_number_missing_gaps VARCHAR(400);
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN fsr_ship_to_party_name_missing_gaps VARCHAR(250);
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN vl_voyage_operator_name_missing_gaps VARCHAR(250);
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET vl_fsr_vsr_voyage_number_missing_gaps = (CASE 
                                      WHEN (bhp_de_tagging = 1 OR bhp_de_tagging = 2) THEN 999
                                      ELSE vl_fsr_vsr_voyage_number
                                      END);
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET vl_fsr_vsr_nomination_number_missing_gaps = (CASE 
                                      WHEN (bhp_de_tagging = 1 OR bhp_de_tagging = 2) THEN 'NOT APPLICABLE'
                                      ELSE vl_fsr_vsr_nomination_number
                                      END);
                                      
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET fsr_ship_to_party_name_missing_gaps = (CASE 
                                      WHEN (bhp_de_tagging = 1 OR bhp_de_tagging = 2) THEN 'NOT APPLICABLE'
                                      ELSE fsr_ship_to_party_name
                                      END);
                                      
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET vl_voyage_operator_name_missing_gaps = (CASE 
                                      WHEN (bhp_de_tagging = 1 OR bhp_de_tagging = 2) THEN 'NOT APPLICABLE'
                                      ELSE vl_voyage_operator_name
                                      END);
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp DROP COLUMN vl_fsr_vsr_voyage_number;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp DROP COLUMN vl_fsr_vsr_nomination_number;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp DROP COLUMN fsr_ship_to_party_name;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp DROP COLUMN vl_voyage_operator_name;
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp RENAME COLUMN vl_fsr_vsr_voyage_number_missing_gaps TO vl_fsr_vsr_voyage_number;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp RENAME COLUMN vl_fsr_vsr_nomination_number_missing_gaps TO vl_fsr_vsr_nomination_number;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp RENAME COLUMN fsr_ship_to_party_name_missing_gaps TO fsr_ship_to_party_name;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp RENAME COLUMN vl_voyage_operator_name_missing_gaps TO vl_voyage_operator_name;
                                      
                                      
                                      -- Adding Geo Status
                                      DROP TABLE IF EXISTS ",input_schema,".geo_status_temp;
                                      
                                      CREATE TABLE ",input_schema,".geo_status_temp as
                                      select a.*, b.ais_geo_status 
                                      FROM ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp a left JOIN ",input_schema,".ais_nav_status b
                                      on a.ais_position_navstatus = b.ais_nav_status;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp;
                                      
                                      ALTER TABLE ",input_schema,".geo_status_temp rename to AIS_VL_FSR_VSR_Daily_Spotfire_temp;
                                      
                                      VACUUM ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp;
                                      
                                      
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('ais_vl_fsr_vsr_daily_spotfire', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    # Close the channel
    odbcClose(channel)
    
    #sys.sleep(10)
    
    
    
    #*********************************** Filename: 5. AIS Future State Handling missing data 20171016.R
    # print("5. AIS Future State Handling missing data 20171016.R")
    print(paste0("5. AIS Future State Handling missing data ",today_date,".R"))
    
    # Data smoothing, performing row wise operations to pull down the data
    
    
    # Channel is opened
    channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
    
    # Obtain the current and previous days data
    master_dump_daily = sqlQuery(channel, paste0("SELECT *
                                                 FROM
                                                 (SELECT DISTINCT *
                                                 FROM ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ---current date
                                                 ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_date_position ASC)
                                                 UNION
                                                 (SELECT DISTINCT *
                                                 FROM ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                                 WHERE ais_vl_fsr_vsr_date_position = '",previous_date_formatted,"' ---previous date
                                                 ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_date_position ASC)
                                                 ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_vessel_name ASC, ais_vl_fsr_vsr_date_position ASC;"))
    
    # Close the channel
    odbcClose(channel)
    
    
    master_dump_daily_dt <- data.table(master_dump_daily)
    
    # drop down the necessary columns
    master_dump_daily_dt[, flag_tag_detag_combine := na.locf(flag_tag_detag_combine, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, flag_bhp := na.locf(flag_bhp, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    
    master_dump_daily_dt[, ais_geodetails_status := na.locf(ais_geodetails_status, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, ais_position_cog := na.locf(ais_position_cog, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, ais_position_hdg := na.locf(ais_position_hdg, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, ais_position_lat := na.locf(ais_position_lat, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, ais_position_lon := na.locf(ais_position_lon, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, ais_position_navstatus := na.locf(ais_position_navstatus, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, ais_position_sog := na.locf(ais_position_sog, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, ais_shiptype := na.locf(ais_shiptype, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, ais_static_name := na.locf(ais_static_name, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, ais_destination := na.locf(ais_destination, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    
    master_dump_daily_dt[, ais_vl_fsr_vsr_vessel_name := na.locf(ais_vl_fsr_vsr_vessel_name, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, ais_vl_fsr_vsr_commodity := na.locf(ais_vl_fsr_vsr_commodity, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, ais_vl_fsr_vsr_incoterms := na.locf(ais_vl_fsr_vsr_incoterms, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, fsr_ship_to_party_name := na.locf(fsr_ship_to_party_name, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, vl_vessel_dwt := na.locf(vl_vessel_dwt, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, vl_voyage_operator_name := na.locf(vl_voyage_operator_name, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, ais_voyage_eta_f := na.locf(ais_voyage_eta_f, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, ais_voyage_dest := na.locf(ais_voyage_dest, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    master_dump_daily_dt[, ais_last_updated_on_date := na.locf(ais_last_updated_on_date, fromLast = FALSE, na.rm=FALSE), by = .(ais_vl_fsr_vsr_imo)]
    
    # Channel is opened
    channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
    
    # Remove the records for the current date, so we can append the data after smoothing
    sqlQuery(channel, paste0("TRUNCATE TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp;"))
    
    # Close the channel
    odbcClose(channel)
    
    # write to temp file
    tempFileStorage <- rawConnection(raw(0),"r+")
    
    write.table(master_dump_daily_dt,tempFileStorage,sep=",",row.names = F, col.names=F)
    
    filename_master_daily = paste0("master_dump_daily_dt.csv")
    
    # Upload to S3
    put_object(file = rawConnectionValue(tempFileStorage), object = paste0(input_s3_bucket_to_store_data,filename_master_daily), bucket = input_bucket)
    
    # close temporary connection
    close(tempFileStorage)
    
    # Channel is opened
    channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
    
    # Append the newly created data 
    src_data_name <- filename_master_daily
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("DROP TABLE IF EXISTS ",input_schema,".ais_combined_daily_temp;
                                      
                                      CREATE TABLE ",input_schema,".ais_combined_daily_temp AS
                                      SELECT TOP 0 *
                                      FROM ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp;
                                      
                                      
                                      COPY ",input_schema,".ais_combined_daily_temp
                                      FROM '",input_s3_bucket_to_store_data,filename_master_daily,"'
                                      access_key_id '",input_aws_access_key_id,"'
                                      secret_access_key '",input_aws_secret_access_key,"'
                                      NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp;
                                      
                                      CREATE TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp AS 
                                      SELECT *
                                      FROM ",input_schema,".ais_combined_daily_temp
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"';"), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('ais_data_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    # Close the channel
    odbcClose(channel)
    
    
    
    #sys.sleep(10)
    
    
    
    
    #*********************************** Filename: 6. AIS Future State Handling missing data 20171016.sql
    # print("6. AIS Future State Handling missing data 20171016.sql")
    print(paste0("6.AIS Future State Handling missing data ",today_date,".SQL"))
    
    # Description: The data has been smoothed, and this code helps to fill in the missing gaps, and rerun tagging and detagging logic where smoothing has been performed
    # Dependencies: This should be run after running R code "5. AIS Future State (Handling missing data).R"
    # Assumptions: NA
    
    # Channel is opened
    channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
    
    
    src_data_name <- 'AIS_VL_FSR_VSR_Daily_Spotfire_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET flag_bhp = 'Non-BHP'
                                      WHERE (flag_tag_detag_combine = 0 OR flag_tag_detag_combine = 2 OR flag_tag_detag_combine IS NULL);
                                      
                                      
                                      -- Rerun tagging
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET bhp_tagging = (CASE
                                      WHEN vl_flag = 1 THEN 1
                                      WHEN ais_vl_fsr_vsr_incoterms = 'CFR' OR ais_vl_fsr_vsr_incoterms = 'CIF' or ais_vl_fsr_vsr_incoterms = 'CFR*'
                                      OR ais_vl_fsr_vsr_incoterms = 'CIF*' or ais_vl_fsr_vsr_incoterms = 'CFR/CIF*' THEN 1 
                                      WHEN fsr_flag = 1 AND ais_vl_fsr_vsr_incoterms = 'FOB' AND fsr_eta_date <= DATEADD(day, 20, fsr_selected_date) THEN 1 
                                      ELSE 0
                                      END);
                                      
                                      
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET bhp_de_tagging = (CASE
                                      WHEN vsr_flag = 1 AND VSR_Departure_Date < vsr_selected_date AND vsr_departure_date_version = 'A' AND vsr_final_disport_flag = 1 THEN 1
                                      WHEN fsr_flag = 1 AND ais_vl_fsr_vsr_incoterms = 'FOB' AND fsr_bol_date < DATEADD(day, -10, fsr_selected_date) THEN 1
                                      ELSE 0
                                      END);
                                      
                                      -- Fill the missings gaps, and rerun the logic to get the new commodity and new bhp flags
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN flag_tag_detag_combine_missing_gaps INTEGER;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN flag_bhp_missing_gaps VARCHAR(250);
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET flag_tag_detag_combine_missing_gaps = (CASE 
                                      WHEN flag_tag_detag_combine = 0 AND bhp_tagging = 1 AND bhp_de_tagging = 0 THEN 1 
                                      ELSE flag_tag_detag_combine
                                      END);
                                      
                                      
                                      
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET flag_bhp_missing_gaps = (CASE
                                      WHEN flag_tag_detag_combine = 0 AND flag_tag_detag_combine_missing_gaps = 1 AND UPPER(ais_vl_fsr_vsr_commodity) LIKE '%IRON%' THEN 'BHP-Iron Ore'
                                      WHEN flag_tag_detag_combine = 0 AND flag_tag_detag_combine_missing_gaps = 1 AND UPPER(ais_vl_fsr_vsr_commodity) LIKE '%COAL%' THEN 'BHP-Coal'
                                      WHEN flag_tag_detag_combine = 0 AND flag_tag_detag_combine_missing_gaps = 1 AND UPPER(ais_vl_fsr_vsr_commodity) LIKE '%COPPER%' THEN 'BHP-Copper'
                                      WHEN flag_tag_detag_combine = 0 AND flag_tag_detag_combine_missing_gaps = 1 THEN 'BHP-Others'
                                      ELSE flag_bhp
                                      END);
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp DROP COLUMN flag_tag_detag_combine;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp DROP COLUMN flag_bhp;
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp RENAME COLUMN flag_tag_detag_combine_missing_gaps TO flag_tag_detag_combine;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp RENAME COLUMN flag_bhp_missing_gaps TO flag_bhp;
                                      
                                      
                                      
                                      -- Adjusting commodity and incoterms non-bhp nulls and missing gaps (NA)
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN ais_vl_fsr_vsr_commodity_missing_gaps VARCHAR(250);
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET ais_vl_fsr_vsr_commodity_missing_gaps = (CASE 
                                      WHEN (flag_tag_detag_combine = 0 OR flag_tag_detag_combine = 2) THEN NULL
                                      WHEN ais_vl_fsr_vsr_commodity = 'NOT APPLICABLE' THEN NULL
                                      WHEN UPPER(ais_vl_fsr_vsr_commodity) LIKE '%IRON%' THEN 'IRON ORE'
                                      WHEN UPPER(ais_vl_fsr_vsr_commodity) LIKE '%COAL%' THEN 'COAL'
                                      WHEN UPPER(ais_vl_fsr_vsr_commodity) LIKE '%COPPER%' THEN 'COPPER'
                                      ELSE UPPER(ais_vl_fsr_vsr_commodity)
                                      END);
                                      
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN ais_vl_fsr_vsr_incoterms_missing_gaps VARCHAR(250);
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET ais_vl_fsr_vsr_incoterms_missing_gaps = (CASE 
                                      WHEN (flag_tag_detag_combine = 0 OR flag_tag_detag_combine = 2) THEN NULL
                                      WHEN ais_vl_fsr_vsr_incoterms = 'NOT APPLICABLE' THEN NULL
                                      WHEN UPPER(ais_vl_fsr_vsr_incoterms) = 'CFR*' THEN 'CFR'
                                      WHEN UPPER(ais_vl_fsr_vsr_incoterms) = 'CIF*' THEN 'CIF'
                                      WHEN UPPER(ais_vl_fsr_vsr_incoterms) = 'CFR/CIF*' THEN 'CFR/CIF'
                                      ELSE UPPER(ais_vl_fsr_vsr_incoterms)
                                      END);
                                      
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp DROP COLUMN ais_vl_fsr_vsr_commodity;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp DROP COLUMN ais_vl_fsr_vsr_incoterms;
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp RENAME COLUMN ais_vl_fsr_vsr_commodity_missing_gaps TO ais_vl_fsr_vsr_commodity;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp RENAME COLUMN ais_vl_fsr_vsr_incoterms_missing_gaps TO ais_vl_fsr_vsr_incoterms;
                                      
                                      
                                      -- Adjusting voyage number, nomination number, ship to party and voyage operator nulls and missing gaps (NA)
                                      
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN vl_fsr_vsr_voyage_number_missing_gaps bigint;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN vl_fsr_vsr_nomination_number_missing_gaps VARCHAR(400);
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN fsr_ship_to_party_name_missing_gaps VARCHAR(250);
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN vl_voyage_operator_name_missing_gaps VARCHAR(250);
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET vl_fsr_vsr_voyage_number_missing_gaps = (CASE 
                                      WHEN vl_fsr_vsr_voyage_number = 999 THEN NULL
                                      WHEN (flag_tag_detag_combine = 0 OR flag_tag_detag_combine = 2) THEN NULL
                                      ELSE vl_fsr_vsr_voyage_number
                                      END);
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET vl_fsr_vsr_nomination_number_missing_gaps = (CASE 
                                      WHEN vl_fsr_vsr_nomination_number = 'NOT APPLICABLE' THEN NULL
                                      WHEN (flag_tag_detag_combine = 0 OR flag_tag_detag_combine = 2) THEN NULL
                                      ELSE vl_fsr_vsr_nomination_number
                                      END);
                                      
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET fsr_ship_to_party_name_missing_gaps = (CASE 
                                      WHEN fsr_ship_to_party_name = 'NOT APPLICABLE' THEN NULL
                                      WHEN (flag_tag_detag_combine = 0 OR flag_tag_detag_combine = 2) THEN NULL
                                      ELSE fsr_ship_to_party_name
                                      END);
                                      
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET vl_voyage_operator_name_missing_gaps = (CASE 
                                      WHEN vl_voyage_operator_name = 'NOT APPLICABLE' THEN NULL
                                      WHEN (flag_tag_detag_combine = 0 OR flag_tag_detag_combine = 2) THEN NULL
                                      ELSE vl_voyage_operator_name
                                      END);
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp DROP COLUMN vl_fsr_vsr_voyage_number;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp DROP COLUMN vl_fsr_vsr_nomination_number;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp DROP COLUMN fsr_ship_to_party_name;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp DROP COLUMN vl_voyage_operator_name;
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp RENAME COLUMN vl_fsr_vsr_voyage_number_missing_gaps TO vl_fsr_vsr_voyage_number;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp RENAME COLUMN vl_fsr_vsr_nomination_number_missing_gaps TO vl_fsr_vsr_nomination_number;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp RENAME COLUMN fsr_ship_to_party_name_missing_gaps TO fsr_ship_to_party_name;
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp RENAME COLUMN vl_voyage_operator_name_missing_gaps TO vl_voyage_operator_name;
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp RENAME COLUMN ais_geo_status TO ais_geo_status_temp;
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp ADD COLUMN ais_geo_status varchar(255);
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp
                                      SET ais_geo_status = ais_geo_status_temp;
                                      
                                      ALTER TABLE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp DROP COLUMN ais_geo_status_temp;
                                      
                                      INSERT INTO ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SELECT DISTINCT *
                                      FROM ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire_temp;
                                      
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('ais_vl_fsr_vsr_daily_spotfire', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    
    #Detagging Cleanup Logic - part 2
    
    src_data_name <- 'AIS_VL_FSR_VSR_Daily_Spotfire'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("----------------------CFR
                                      ----- Updating the table to apply the detagging cleanup (need to key in the date)
                                      
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET flag_tag_detag_combine = 2
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND vl_fsr_vsr_voyage_number IN (WITH voy_no_to_detag AS
                                      (SELECT DISTINCT vl_fsr_vsr_voyage_number_raw
                                      FROM ",input_schema,".ais_vl_fsr_vsr_daily_spotfire
                                      WHERE vl_fsr_vsr_voyage_number_raw IS NOT NULL
                                      AND flag_bhp LIKE 'BHP-%'
                                      AND (ais_vl_fsr_vsr_incoterms LIKE '%CFR%' OR ais_vl_fsr_vsr_incoterms LIKE '%CIF%')
                                      AND ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      ORDER BY vl_fsr_vsr_voyage_number_raw)
                                      SELECT *
                                      FROM voy_no_to_detag
                                      WHERE vl_fsr_vsr_voyage_number_raw IN (WITH voy_no_min_detag AS
                                      (SELECT DISTINCT vl_fsr_vsr_voyage_number_raw, flag_bhp, ais_vl_fsr_vsr_date_position
                                      FROM ",input_schema,".ais_vl_fsr_vsr_daily_spotfire
                                      WHERE vl_fsr_vsr_voyage_number_raw IS NOT NULL
                                      ORDER BY vl_fsr_vsr_voyage_number_raw, ais_vl_fsr_vsr_date_position),
                                      
                                      min_detag_less_today AS
                                      (SELECT vl_fsr_vsr_voyage_number_raw, flag_bhp, MIN(ais_vl_fsr_vsr_date_position) as min_detag_date
                                      FROM voy_no_min_detag
                                      WHERE flag_bhp LIKE 'Non-BHP'
                                      GROUP BY vl_fsr_vsr_voyage_number_raw, flag_bhp
                                      ORDER BY vl_fsr_vsr_voyage_number_raw, MIN(ais_vl_fsr_vsr_date_position)),
                                      
                                      list_voy_number AS
                                      (SELECT vl_fsr_vsr_voyage_number_raw, min_detag_date, ('",today_date_formatted,"' - min_detag_date) as diff
                                      FROM min_detag_less_today
                                      WHERE ('",today_date_formatted,"' - min_detag_date) >= 1)
                                      
                                      SELECT vl_fsr_vsr_voyage_number_raw
                                      FROM list_voy_number));
                                      
                                      --------------------- FOB
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET flag_tag_detag_combine = 2
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND vl_fsr_vsr_nomination_number IN (WITH nom_no_to_detag AS
                                      (SELECT DISTINCT vl_fsr_vsr_nomination_number_raw
                                      FROM ",input_schema,".ais_vl_fsr_vsr_daily_spotfire
                                      WHERE vl_fsr_vsr_nomination_number_raw IS NOT NULL
                                      AND flag_bhp LIKE 'BHP-%'
                                      AND ais_vl_fsr_vsr_incoterms LIKE '%FOB%'
                                      AND ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      ORDER BY vl_fsr_vsr_nomination_number_raw)
                                      SELECT *
                                      FROM nom_no_to_detag
                                      WHERE vl_fsr_vsr_nomination_number_raw IN (WITH nom_no_min_detag AS
                                      (SELECT DISTINCT vl_fsr_vsr_nomination_number_raw, flag_bhp, ais_vl_fsr_vsr_date_position
                                      FROM ",input_schema,".ais_vl_fsr_vsr_daily_spotfire
                                      WHERE vl_fsr_vsr_nomination_number_raw IS NOT NULL
                                      ORDER BY vl_fsr_vsr_nomination_number_raw, ais_vl_fsr_vsr_date_position),
                                      
                                      min_detag_less_today AS
                                      (SELECT vl_fsr_vsr_nomination_number_raw, flag_bhp, MIN(ais_vl_fsr_vsr_date_position) as min_detag_date
                                      FROM nom_no_min_detag
                                      WHERE flag_bhp LIKE 'Non-BHP'
                                      GROUP BY vl_fsr_vsr_nomination_number_raw, flag_bhp
                                      ORDER BY vl_fsr_vsr_nomination_number_raw, MIN(ais_vl_fsr_vsr_date_position)),
                                      
                                      list_nom_number AS
                                      (SELECT vl_fsr_vsr_nomination_number_raw, min_detag_date, ('",today_date_formatted,"' - min_detag_date) as diff
                                      FROM min_detag_less_today
                                      WHERE ('",today_date_formatted,"' - min_detag_date) >= 1)
                                      
                                      SELECT vl_fsr_vsr_nomination_number_raw
                                      FROM list_nom_number));
                                      
                                      
                                      
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET flag_bhp = 'Non-BHP'
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND flag_tag_detag_combine = 2;
                                      
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET ais_vl_fsr_vsr_incoterms = NULL
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND flag_tag_detag_combine = 2;
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET ais_vl_fsr_vsr_commodity = NULL
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND flag_tag_detag_combine = 2;
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET vl_fsr_vsr_voyage_number = NULL
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND flag_tag_detag_combine = 2;
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET vl_fsr_vsr_nomination_number = NULL
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND flag_tag_detag_combine = 2;
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET fsr_ship_to_party_name = NULL
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND flag_tag_detag_combine = 2;
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET vl_voyage_operator_name = NULL
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND flag_tag_detag_combine = 2;



                                      DROP TABLE IF EXISTS ",input_schema,".detagging_cleanup_part_3;





                                  CREATE TABLE ",input_schema,".detagging_cleanup_part_3 AS
                                      WITH imo_tagged_since_beginning AS
                                      (SELECT DISTINCT
                                      ais_vl_fsr_vsr_imo,
                                      MIN(ais_vl_fsr_vsr_date_position) as ais_vl_fsr_vsr_date_position,
                                      ais_vl_fsr_vsr_incoterms,
                                      ais_vl_fsr_vsr_commodity,
                                      flag_bhp
                                      FROM
                                      (SELECT DISTINCT
                                      sub_3.*
                                      FROM
                                      (SELECT DISTINCT
                                      ais_vl_fsr_vsr_imo,
                                      COUNT(*) as count_num_rows
                                      FROM
                                      (SELECT DISTINCT
                                      ais_vl_fsr_vsr_imo,
                                      flag_bhp,
                                      COUNT(*) as count_per_flag
                                      FROM
                                      ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      WHERE 
                                      ais_vl_fsr_vsr_imo IN (SELECT DISTINCT 
                                      ais_vl_fsr_vsr_imo
                                      FROM 
                                      ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      WHERE 
                                      ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND 
                                      flag_bhp LIKE 'BHP-%'
                                      AND 
                                      vl_fsr_vsr_null_flag = 1)
                                      GROUP BY 
                                      ais_vl_fsr_vsr_imo,
                                      flag_bhp) as sub_1
                                      GROUP BY
                                      ais_vl_fsr_vsr_imo
                                      HAVING
                                      COUNT(*) = 1) as sub_2
                                      LEFT JOIN 
                                      (SELECT DISTINCT 
                                      a1.ais_vl_fsr_vsr_imo, 
                                      MIN(a1.ais_vl_fsr_vsr_date_position) as ais_vl_fsr_vsr_date_position, 
                                      a1.ais_vl_fsr_vsr_incoterms,
                                      a1.ais_vl_fsr_vsr_commodity,
                                      a1.flag_bhp, 
                                      a1.vl_fsr_vsr_null_flag
                                      FROM 
                                      ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire as a1
                                      GROUP BY
                                      a1.ais_vl_fsr_vsr_imo,
                                      a1.ais_vl_fsr_vsr_incoterms,
                                      a1.ais_vl_fsr_vsr_commodity,
                                      a1.flag_bhp, 
                                      a1.vl_fsr_vsr_null_flag
                                      ) as sub_3
                                      ON sub_2.ais_vl_fsr_vsr_imo = sub_3.ais_vl_fsr_vsr_imo) as sub_4
                                      GROUP BY
                                      ais_vl_fsr_vsr_imo,
                                      ais_vl_fsr_vsr_incoterms,
                                      ais_vl_fsr_vsr_commodity,
                                      flag_bhp),
                                      
                                      
                                      imo_tagged_and_detagged AS
                                      (SELECT DISTINCT
                                      sub_2.ais_vl_fsr_vsr_imo,
                                      sub_2.ais_vl_fsr_vsr_date_position,
                                      sub_2.ais_vl_fsr_vsr_incoterms,
                                      sub_2.ais_vl_fsr_vsr_commodity,
                                      sub_2.flag_bhp
                                      FROM
                                      (SELECT DISTINCT
                                      sub_1.*,
                                      DENSE_RANK() OVER (PARTITION BY ais_vl_fsr_vsr_imo ORDER BY ais_vl_fsr_vsr_date_position DESC) as rank
                                      FROM
                                      (SELECT DISTINCT 
                                      ais_vl_fsr_vsr_imo, 
                                      ais_vl_fsr_vsr_date_position, 
                                      ais_vl_fsr_vsr_incoterms,
                                      ais_vl_fsr_vsr_commodity,
                                      flag_bhp, 
                                      vl_fsr_vsr_null_flag,
                                      LAG(flag_bhp,1) OVER (PARTITION BY ais_vl_fsr_vsr_imo ORDER BY ais_vl_fsr_vsr_date_position) as prev_flag_bhp
                                      FROM 
                                      ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      WHERE 
                                      --ais_vl_fsr_vsr_imo = '9619555' OR ais_vl_fsr_vsr_imo = '9696759' or ais_vl_fsr_vsr_imo = '9705299' or ais_vl_fsr_vsr_imo = '9537848'
                                      ais_vl_fsr_vsr_imo IN (SELECT DISTINCT 
                                      ais_vl_fsr_vsr_imo
                                      FROM 
                                      ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      WHERE 
                                      ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND 
                                      flag_bhp LIKE 'BHP-%'
                                      AND 
                                      vl_fsr_vsr_null_flag = 1)
                                      ) as sub_1
                                      WHERE
                                      NOT (flag_bhp = prev_flag_bhp)) as sub_2
                                      WHERE rank = 1
                                      ),
                                      
                                      list_imo_to_detag AS
                                      (SELECT DISTINCT 
                                      *
                                      FROM
                                      (SELECT DISTINCT
                                      *
                                      FROM imo_tagged_since_beginning)
                                      UNION
                                      (SELECT DISTINCT
                                      *
                                      FROM imo_tagged_and_detagged)),
                                      
                                      imo_combined AS
                                      (SELECT 
                                      ais_vl_fsr_vsr_imo,
                                      ais_vl_fsr_vsr_date_position,
                                      (CASE
                                      WHEN UPPER(ais_vl_fsr_vsr_incoterms) LIKE '%CIF%' OR UPPER(ais_vl_fsr_vsr_incoterms) LIKE '%CFR%' THEN 'CFR'
                                      WHEN UPPER(ais_vl_fsr_vsr_incoterms) LIKE '%FOB%' THEN 'FOB'
                                      ELSE NULL
                                      END) as incoterms,
                                      (CASE 
                                      WHEN UPPER(ais_vl_fsr_vsr_commodity) LIKE '%IRON%' THEN 'IRON ORE' 
                                      WHEN UPPER(ais_vl_fsr_vsr_commodity) LIKE '%COAL%' THEN 'COAL'
                                      WHEN UPPER(ais_vl_fsr_vsr_commodity) LIKE '%COPPER%' THEN 'COPPER'
                                      ELSE 'OTHER'
                                      END) as commodity,
                                      '1'::integer as helper_flag,
                                      '",today_date_formatted,"'::date as today_date,
                                      ('",today_date_formatted,"' - ais_vl_fsr_vsr_date_position) as num_of_days_since_prev_tag
                                      FROM list_imo_to_detag)
                                      
                                      SELECT DISTINCT
                                      x.*,
                                      y.total_time
                                      FROM imo_combined as x
                                      LEFT JOIN ",input_schema,".detagging_cutoff_thresholds as y
                                      ON UPPER(x.incoterms) = UPPER(y.incoterms) 
                                      AND UPPER(x.commodity) = UPPER(y.commodity)
                                      WHERE 
                                      (num_of_days_since_prev_tag > y.total_time);
                                      
                                      
                                      
                                      
                                      
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET flag_tag_detag_combine = 3
                                      WHERE ais_vl_fsr_vsr_imo IN (SELECT DISTINCT ais_vl_fsr_vsr_imo
                                      FROM ",input_schema,".detagging_cleanup_part_3)
                                      AND ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire.ais_vl_fsr_vsr_date_position = '",today_date_formatted,"';
                                      
                                      
                                      
                                      
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET flag_bhp = 'Non-BHP'
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND flag_tag_detag_combine = 3;
                                      
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET ais_vl_fsr_vsr_incoterms = NULL
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND flag_tag_detag_combine = 3;
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET ais_vl_fsr_vsr_commodity = NULL
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND flag_tag_detag_combine = 3;
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET vl_fsr_vsr_voyage_number = NULL
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND flag_tag_detag_combine = 3;
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET vl_fsr_vsr_nomination_number = NULL
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND flag_tag_detag_combine = 3;
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET fsr_ship_to_party_name = NULL
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND flag_tag_detag_combine = 3;
                                      
                                      UPDATE ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      SET vl_voyage_operator_name = NULL
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'
                                      AND flag_tag_detag_combine = 3;
                                      
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".detagging_cleanup_part_3;
                                      
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('ais_vl_fsr_vsr_daily_spotfire', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    
    
    
    
    #################################### Section 12: Calculate fields required for Daily AIS Dashboard views (1-3)
    
    
    
    # Creation of a table to perform calculations like last port, hours to port, etc.
    src_data_name <- 'AIS_VL_FSR_VSR_Daily_Spotfire'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("DROP TABLE IF EXISTS ",input_schema,".ais_sap_combined_calc_temp; 
                                      
                                      CREATE TABLE ",input_schema,".ais_sap_combined_calc_temp AS
                                      SELECT DISTINCT ais_vl_fsr_vsr_date_position, 
                                      ais_geodetails_status,
                                      ais_geo_status,
                                      ais_position_cog, 
                                      ais_position_hdg, 
                                      ais_position_lat, 
                                      ais_position_lon, 
                                      ais_position_navstatus,
                                      ais_position_sog, 
                                      ais_shiptype, 
                                      ais_vl_fsr_vsr_imo, 
                                      ais_vl_fsr_vsr_vessel_name, --ais_static_name, 
                                      ais_vl_fsr_vsr_commodity,
                                      ais_vl_fsr_vsr_incoterms, 
                                      fsr_freight_voyage_number,
                                      fsr_nomination_number,
                                      fsr_ship_to_party_name, 
                                      vl_vessel_dwt, 
                                      vl_voyage_operator_name, 
                                      ais_voyage_eta_f,
                                      flag_bhp, 
                                      ais_voyage_dest,
                                      ais_last_updated_on_date,
                                      vl_fsr_vsr_voyage_number, 
                                      vl_fsr_vsr_nomination_number, 
                                      vl_fsr_vsr_voyage_number_raw,
                                      vl_fsr_vsr_nomination_number_raw,
                                      ais_destination,
                                      ais_vl_fsr_vsr_imo_date_key,
                                      vl_vessel_eta,
                                      vl_vendor_name,
                                      vl_voyage_operator,
                                      fsr_material_name,
                                      fsr_eta_date
                                      FROM ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"';
                                      
                                      -- ************************************************************************************************************************************
                                      -- Calculation 3 : Last Port
                                      
                                      -- ############################################## Port Congestion #######################################################################
                                      
                                      -- Calculation 4 : 
                                      -- Adding columns 
                                      -- 1) Days to Port
                                      -- 2) Days at Port
                                      -- 3) Hours to Port
                                      -- 4) Hours at Port
                                      
                                      
                                      
                                      VACUUM ",input_schema,".ais_sap_combined_calc_temp;
                                      ALTER TABLE ",input_schema,".ais_sap_combined_calc_temp add column days_to_port numeric;
                                      ALTER TABLE ",input_schema,".ais_sap_combined_calc_temp add column hours_to_port numeric;
                                      ALTER TABLE ",input_schema,".ais_sap_combined_calc_temp add column days_at_port numeric default 0;
                                      ALTER TABLE ",input_schema,".ais_sap_combined_calc_temp add column hours_at_port numeric default 0;
                                      
                                      
                                      UPDATE ",input_schema,".ais_sap_combined_calc_temp
                                      set days_to_port = datediff(days,ais_vl_fsr_vsr_date_position,ais_voyage_eta_f); 
                                      
                                      
                                      UPDATE ",input_schema,".ais_sap_combined_calc_temp
                                      set hours_to_port = datediff(hours,ais_vl_fsr_vsr_date_position,ais_voyage_eta_f);
                                      
                                      
                                      UPDATE ",input_schema,".ais_sap_combined_calc_temp
                                      set days_at_port = case when days_to_port>0 then 0 else (days_to_port*(-1)+1) end;
                                      
                                      
                                      UPDATE ",input_schema,".ais_sap_combined_calc_temp
                                      set hours_at_port = case when hours_to_port>0 then 0 else (hours_to_port*(-1)) end; 
                                      
                                      
                                      ALTER TABLE ",input_schema,".ais_sap_combined_calc DROP COLUMN lead;
                                      ALTER TABLE ",input_schema,".ais_sap_combined_calc DROP COLUMN dc_change_lead;
                                      
                                      INSERT INTO ",input_schema,".ais_sap_combined_calc
                                      SELECT DISTINCT *
                                      FROM ",input_schema,".ais_sap_combined_calc_temp;
                                      
                                      VACUUM ",input_schema,".ais_sap_combined_calc;
                                      
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('ais_vl_fsr_vsr_daily_spotfire', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    # Close the channel
    odbcClose(channel)
    
    #sys.sleep(10)
    
    
    #*********************************** Filename: 7. AIS Future State Calculations 20171016.R
    # print("7. AIS Future State Calculations 20171016.R")
    print(paste0("7. AIS Future State Calculations ",today_date,".R"))
    
    # Channel is opened
    channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
    
    
    aisdt = sqlQuery(channel, paste0("select distinct * 
                                     FROM ",input_schema,".ais_sap_combined_calc;"))
    
    
    # Close the channel
    odbcClose(channel)
    
    #********************************************************** Voyage IDs **********************************************************#
    
    # Selecting the relevant column for the calculation of last port
    aistemp = aisdt %>% 
      select(ais_vl_fsr_vsr_imo,ais_vl_fsr_vsr_date_position,ais_vl_fsr_vsr_imo_date_key,ais_destination,ais_voyage_eta_f,hours_to_port,hours_at_port)
    
    aistemp = unique(aistemp)
    
    # Arranging data by imo and date position
    aistemp = aistemp %>% 
      arrange(ais_vl_fsr_vsr_imo,ais_vl_fsr_vsr_date_position)
    
    # Creating lag of destination by imo
    aistemp = aistemp %>% 
      group_by(ais_vl_fsr_vsr_imo) %>% 
      mutate(ais_destination_lag = lag(ais_destination,1))
    
    # Creating dc_flag to keep a track of the change in location
    
    aistemp$dc_flag = ifelse(aistemp$ais_destination!= aistemp$ais_destination_lag,1,0 )
    aistemp$dc_flag = ifelse(is.na(aistemp$dc_flag),0,aistemp$dc_flag)
    
    # Creating voyage id by imo
    
    aistemp_1 = aistemp %>%
      group_by(ais_vl_fsr_vsr_imo)%>%
      mutate(counter=cumsum(dc_flag==1)+1)
    
    aistemp_1$voyage_id = paste0(aistemp_1$ais_vl_fsr_vsr_imo,aistemp_1$counter)
    colnames(aistemp_1)
    
    #********************************************************** LAST PORT **********************************************************#
    
    # Adding last port, everytime the dc_flag is 1 , we have destination and lag - the lag is the last port
    df_last_port = aistemp_1 %>% 
      filter(dc_flag==1) %>% 
      select(ais_destination_lag,voyage_id)
    
    df_last_port = df_last_port[,-1]
    
    # Merging it back to the dataset
    aistemp_merge = merge(aistemp_1,df_last_port,by="voyage_id",all.x = T)
    
    colnames(aistemp_merge)[12] = "ais_last_port"
    
    # Calculating lane
    aistemp_merge$lane = paste0(aistemp_merge$ais_last_port,'-',aistemp_merge$ais_destination)
    
    # ********************************************** Average time spent at port  **************************************************#
    
    df = sqldf("select voyage_id,ais_destination,max(hours_at_port) as max_hrs FROM aistemp_merge group by voyage_id,ais_destination")
    df_avg_time = sqldf("select ais_destination, avg(max_hrs) as avg_time_at_port FROM df group by ais_destination")
    
    
    # *********************************************** Average time taken to reach next port ************************# 
    
    # REmoving entries with null ETAs
    aistemp_merge = aistemp_merge %>% filter(!is.na(ais_voyage_eta_f))
    colSums(is.na(aistemp_merge))
    
    # sum(aistemp$dc_flag)
    
    # Adding lag of timestamp position
    aistemp_merge = aistemp_merge %>% 
      group_by(ais_vl_fsr_vsr_imo) %>% 
      mutate(ts_lag = lag(ais_vl_fsr_vsr_date_position,1)) 
    
    # Removing null lags
    aistemp_merge = aistemp_merge %>%  filter(!is.na(ts_lag))
    
    # Calculating difference
    aistemp_merge$diff = difftime(aistemp_merge$ais_voyage_eta_f,aistemp_merge$ts_lag,units="hours")
    
    # applying filter for dest change
    dt_ais = aistemp_merge %>% 
      filter(dc_flag==1) %>% 
      select(ais_destination,ais_last_port,lane,diff)
    
    dt_ais = dt_ais %>% filter(diff >0)
    
    # Taking average time
    dt_ais_avg = sqldf("select lane, avg(diff) as avg_time FROM dt_ais group by lane")
    
    
    s_unique = as.data.frame(unique(dt_ais$ais_last_port))
    colnames(s_unique) ="loc"
    d_unique = as.data.frame(unique(dt_ais$ais_destination))
    colnames(d_unique) ="loc"
    
    # When source and destination is same, average time is 0
    dest = rbind(s_unique,d_unique)
    dest =  unique(dest)
    colnames(dest)
    dest$dest = dest$loc
    dest$lane= paste0(dest$dest,"-",dest$loc)
    dest$avg_time = 0
    dest = dest[,c(3,4)]
    
    # Mergin the data back
    dt_f = rbind(dt_ais_avg,dest)
    dt_f = sqldf("select lane,avg(avg_time) as avg_time FROM dt_f group by lane")
    dt_f = unique(dt_f)
    length(unique(dt_f$lane))
    
    
    
    
    distance_table_filename = "distance_table_daily.csv"
    
    # write to temp file
    tempFileStorage <- rawConnection(raw(0),"r+")
    
    write.table(dt_f, tempFileStorage ,sep=",",row.names = F, col.names=F)
    
    put_object(file = rawConnectionValue(tempFileStorage), object = paste0(input_s3_bucket_to_store_data,distance_table_filename), bucket = input_bucket)
    
    # close temporary connection
    close(tempFileStorage)
    
    
    # Channel is opened
    channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
    
    sqlQuery(channel, paste0("DROP TABLE IF EXISTS ",input_schema,".distance_table_daily;
                             CREATE TABLE ",input_schema,".distance_table_daily(
                             lane varchar(400),
                             avg_journey_time numeric);"))
    
    sqlQuery(channel, paste0("COPY ",input_schema,".distance_table_daily 
                             FROM '",input_s3_bucket_to_store_data,"distance_table_daily.csv' 
                             access_key_id '",input_aws_access_key_id,"' 
                             secret_access_key '",input_aws_secret_access_key,"' 
                             NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"))
    
    # Close the channel
    odbcClose(channel)
    
    # ********************************************  WRITING Data *********************************************************************************#
    
    last_port_filename = "last_port_daily.csv"
    
    # write to temp file
    tempFileStorage <- rawConnection(raw(0),"r+")
    
    last_port_final <- unique(aistemp_merge[,c(1,4,12)])
    
    write.table(last_port_final, tempFileStorage ,sep=",",row.names = F, col.names=F)
    
    put_object(file = rawConnectionValue(tempFileStorage), object = paste0(input_s3_bucket_to_store_data,last_port_filename), bucket = input_bucket)
    
    # close temporary connection
    close(tempFileStorage)
    
    channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
    
    sqlQuery(channel, paste0("DROP TABLE IF EXISTS ",input_schema,".last_port_daily ;
                             CREATE TABLE ",input_schema,".last_port_daily(
                             voyage_id varchar(200),
                             ais_vl_fsr_vsr_imo_date_key varchar(200),
                             last_port varchar(200));"))
    
    src_data_name <- 'last_port_daily.csv'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("COPY ",input_schema,".last_port_daily 
                                      FROM '",input_s3_bucket_to_store_data,"last_port_daily.csv' 
                                      access_key_id '",input_aws_access_key_id,"' 
                                      secret_access_key '",input_aws_secret_access_key,"' 
                                      NULL 'NA' IGNOREHEADER 0 ACCEPTINVCHARS CSV;"), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('last_port_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    # Close the channel
    odbcClose(channel)
    
    
    #sys.sleep(10)
    
    
    #*********************************** Filename: 8. AIS Future State Calculations 20171016.sql
    # print("8. AIS Future State Calculations 20171016.sql")
    print(paste0("8. AIS Future State Calculations ",today_date,".SQL"))
    
    # Description: This code merges s5 data and does all the calculations required for eta standardization. This code creates tables that will drive quick view, port congestion and eta comparison
    # Dependencies: NA
    # Assumptions: NA
    
    
    # Channel is opened
    channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
    
    src_data_name <- 'ais_sap_combined_calc'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("DROP TABLE IF EXISTS ",input_schema,".avg_time_spent_temp;
                                      
                                      CREATE TABLE ",input_schema,".avg_time_spent_temp as 
                                      select * 
                                      FROM ",input_schema,".ais_sap_combined_calc 
                                      order by ais_vl_fsr_vsr_imo,ais_vl_fsr_vsr_date_position; 
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".ais_sap_combined_calc;
                                      
                                      
                                      CREATE TABLE ",input_schema,".ais_sap_combined_calc as 
                                      select *, lead(ais_destination) over(partition by ais_vl_fsr_vsr_imo 
                                      order by ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position) 
                                      FROM ",input_schema,".avg_time_spent_temp;
                                      
                                      VACUUM ",input_schema,".ais_sap_combined_calc;
                                      
                                      ALTER TABLE ",input_schema,".ais_sap_combined_calc add column dc_change_lead integer;
                                      
                                      UPDATE ",input_schema,".ais_sap_combined_calc 
                                      set dc_change_lead = (case 
                                      when ais_destination = lead then 0 
                                      else 1 
                                      end);
                                      
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".avg_time_at_port_future;
                                      
                                      CREATE TABLE ",input_schema,".avg_time_at_port_future as 
                                      select ais_destination, avg(days_at_port) 
                                      FROM ",input_schema,".ais_sap_combined_calc 
                                      where dc_change_lead = 1 
                                      group by ais_destination;"), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('avg_time_at_port_u', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU and AIS_HISTORICAL','", src_data_name,"','", end_ts,"')"))
    
    
    
    # Merging of S5, and inserting data into table that will drive the quick view and port congestion
    src_data_name <- 's5_historical_dump_daily'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("DROP TABLE IF EXISTS ",input_schema,".ais_sap_s5_daily_temp;
                                      
                                      -- Merging of S5
                                      CREATE TABLE ",input_schema,".ais_sap_s5_daily_temp AS
                                      SELECT ais.*, 
                                      s5.vessel_status as s5_vessel_status,
                                      s5.date_position as s5_date_position,
                                      s5.dayofmonth as s5_dayofmonth,
                                      s5.operator as s5_operator,
                                      s5.voyage_number as s5_voyage_number,
                                      s5.vessel_name as s5_vessel_name,
                                      s5.port as s5_port,
                                      s5.eta as s5_eta,
                                      s5.etb as s5_etb,
                                      s5.etd as s5_etd,
                                      s5.remark as s5_remark,
                                      s5.loi_accepted as s5_loi_accepted,
                                      s5.risk_of_delay_to_berth as s5_risk_of_delay_to_berth,
                                      s5.reason as s5_reason,
                                      s5.variance_t1 as s5_variance_t1,
                                      s5.eta_port_hedland as s5_eta_port_hedland,
                                      s5.receiver as s5_receiver,
                                      s5.workbook_name as s5_workbook_name,
                                      s5.s5_source_commodity,
                                      s5.s5_vessel_name_cleansed,
                                      s5.s5_vessel_name_date_key,
                                      s5.s5_operator_raw
                                      FROM ",input_schema,".ais_sap_combined_calc ais LEFT JOIN ",input_schema,".s5_historical_dump_daily s5 
                                      ON CONCAT(REGEXP_REPLACE(UPPER(ais.ais_vl_fsr_vsr_vessel_name), '[^a-zA-Z0-9]+', ''),ais.ais_vl_fsr_vsr_date_position) = s5.s5_vessel_name_date_key
                                      WHERE ais_vl_fsr_vsr_date_position =  '",today_date_formatted,"';
                                      
                                      
                                      -- ################################################ Adding Last Port ####################################################################
                                      
                                      -- Refer to the R codes
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".last_port_temp;
                                      
                                      CREATE TABLE ",input_schema,".last_port_temp as
                                      select a.*, b.last_port as ais_last_port, b.voyage_id 
                                      FROM ",input_schema,".ais_sap_s5_daily_temp a left JOIN ",input_schema,".last_port_daily b 
                                      on a.ais_vl_fsr_vsr_imo_date_key = b.ais_vl_fsr_vsr_imo_date_key;
                                      
                                      INSERT INTO ",input_schema,".ais_sap_s5_daily
                                      SELECT DISTINCT *
                                      FROM ",input_schema,".last_port_temp;
                                      
                                      --VACUUM ",input_schema,".ais_sap_s5_daily;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".m_daily_data_combined_temp;
                                      
                                      CREATE TABLE ",input_schema,".m_daily_data_combined_temp AS
                                      SELECT DISTINCT ais_vl_fsr_vsr_date_position, 
                                      ais_geodetails_status, 
                                      ais_geo_status,
                                      ais_position_cog, 
                                      ais_position_hdg, 
                                      ais_position_lat, 
                                      ais_position_lon, 
                                      ais_position_navstatus, 
                                      ais_position_sog, 
                                      ais_shiptype, 
                                      ais_vl_fsr_vsr_imo, 
                                      ais_vl_fsr_vsr_vessel_name, --ais_static_name, 
                                      ais_vl_fsr_vsr_commodity,
                                      ais_vl_fsr_vsr_incoterms, 
                                      fsr_freight_voyage_number,
                                      fsr_nomination_number, 
                                      fsr_ship_to_party_name, 
                                      vl_vessel_dwt, 
                                      vl_voyage_operator_name, 
                                      ais_voyage_eta_f,
                                      flag_bhp,
                                      ais_destination, 
                                      ais_last_updated_on_date,
                                      vl_fsr_vsr_voyage_number,
                                      vl_fsr_vsr_nomination_number,
                                      days_at_port,
                                      days_to_port,
                                      ais_last_port
                                      FROM ",input_schema,".ais_sap_s5_daily
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'; 
                                      
                                      
                                      INSERT INTO ",input_schema,".m_daily_data_combined
                                      SELECT DISTINCT *
                                      FROM ",input_schema,".m_daily_data_combined_temp;
                                      
                                      VACUUM ",input_schema,".m_daily_data_combined;
                                      
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('ais_sap_s5_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    
    #################################### Section 13: ETA Standardization
    
    # ETA standardisation
    src_data_name <- 'ais_sap_s5_daily'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("DROP TABLE IF EXISTS ",input_schema,".eta_standardisation_daily_temp;
                                      
                                      -- Getting relevant columns for eta comparison view
                                      CREATE TABLE ",input_schema,".eta_standardisation_daily_temp as 
                                      select DISTINCT ais_vl_fsr_vsr_vessel_name, --ais_static_name,
                                      ais_vl_fsr_vsr_imo,
                                      ais_vl_fsr_vsr_imo_date_key,
                                      ais_vl_fsr_vsr_date_position,
                                      ais_position_lon,
                                      ais_position_lat,
                                      ais_voyage_eta_f, 
                                      ais_shiptype,
                                      ais_destination,
                                      vl_vessel_eta, 
                                      vl_vendor_name,
                                      vl_voyage_operator_name,
                                      vl_voyage_operator,
                                      s5_operator,
                                      vl_fsr_vsr_nomination_number_raw as vl_fsr_vsr_nomination_number,
                                      ais_vl_fsr_vsr_commodity,
                                      ais_vl_fsr_vsr_incoterms,
                                      fsr_eta_date,
                                      ais_last_port,
                                      voyage_id,
                                      flag_bhp,
                                      ais_geo_status,
                                      days_to_port,
                                      hours_to_port,
                                      days_at_port,
                                      hours_at_port, 
                                      dateadd(day,5, ais_voyage_eta_f) as ais_fixed_eta,
                                      s5_eta, 
                                      dateadd(day,5,s5_eta) as s5_fixed_eta ,
                                      vl_fsr_vsr_voyage_number_raw as vl_fsr_vsr_voyage_number,
                                      fsr_nomination_number,
                                      (CASE WHEN vl_vessel_eta IS NOT NULL THEN vl_vessel_eta
                                      WHEN fsr_eta_date IS NOT NULL THEN fsr_eta_date
                                      ELSE NULL
                                      END) as vl_fsr_vessel_eta
                                      FROM ",input_schema,".ais_sap_s5_daily
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"';
                                      
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".destination_standardization_daily_temp;
                                      
                                      CREATE TABLE ",input_schema,".destination_standardization_daily_temp as 
                                      select *, 
                                      case when UPPER(ais_destination) like 'JINGTANG' then dateadd(day,13, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'CAOFEIDIAN' then dateadd(day,13, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'XINGANG' then dateadd(day,13, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'BAYUQUAN' then dateadd(day,13, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'HUANGHUA' then dateadd(day,13, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'DALIAN' then dateadd(day,12, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'QINGDAO' then dateadd(day,12, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'RIZHAO' then dateadd(day,12, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'YANZI' then dateadd(day,11, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'BEILUN' then dateadd(day,11, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'SHANGHAI' then dateadd(day,11, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'TAICANG' then dateadd(day,11, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'NANTONG' then dateadd(day,11, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'HAILI' then dateadd(day,11, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'JIANYIN' then dateadd(day,11, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'LIANYUNGANG' then dateadd(day,11, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'YANTAI' then dateadd(day,11, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'ZHANGJIAGANG' then dateadd(day,11, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'ZHOUSHAN' then dateadd(day,10, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'XIAMEN' then dateadd(day,10, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'ZHANJIANG' then dateadd(day,10, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'FANGCHENG' then dateadd(day,10, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'HONGKONG' then dateadd(day,10, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'KEMEN' then dateadd(day,10, ais_fixed_eta)
                                      when UPPER(ais_destination) like 'PORT HEDLAND' then ais_voyage_eta_f  
                                      when UPPER(ais_destination) like 'AU PH' then ais_voyage_eta_f
                                      else NULL
                                      end as fixed_eta_to_ph 
                                      FROM ",input_schema,".eta_standardisation_daily_temp;
                                      
                                      
                                      
                                      ALTER TABLE ",input_schema,".destination_standardization_daily_temp rename column ais_fixed_eta to fixed_etd;
                                      
                                      -- Adding next port as 'PORT HEDLAND' to all the records
                                      ALTER TABLE ",input_schema,".destination_standardization_daily_temp add column next_port varchar(200);
                                      
                                      UPDATE ",input_schema,".destination_standardization_daily_temp
                                      set next_port='PORT HEDLAND';
                                      
                                      -- Adding Lane to get the average time to port hedland
                                      ALTER TABLE ",input_schema,".destination_standardization_daily_temp add column lane varchar(400);
                                      
                                      UPDATE ",input_schema,".destination_standardization_daily_temp
                                      set lane = ais_destination || '-' || next_port;
                                      
                                      -- Adding average time spent at port
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".avg_time_spent_at_port_temp;
                                      
                                      CREATE TABLE ",input_schema,".avg_time_spent_at_port_temp as 
                                      select a.*,b.avg as avg_time_at_port 
                                      FROM ",input_schema,".destination_standardization_daily_temp a left JOIN ",input_schema,".avg_time_at_port_u b 
                                      on a.ais_destination = b.ais_destination;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".avg_journey_time_temp;
                                      CREATE TABLE ",input_schema,".avg_journey_time_temp as 
                                      select a.*,b.avg_journey_time 
                                      FROM ",input_schema,".avg_time_spent_at_port_temp a left JOIN ",input_schema,".distance_table_daily b 
                                      on a.lane = b.lane;
                                      
                                      ALTER TABLE ",input_schema,".avg_journey_time_temp add column additional_time numeric;
                                      ALTER TABLE ",input_schema,".avg_journey_time_temp add column profiled_eta_to_ph timestamp;
                                      
                                      UPDATE ",input_schema,".avg_journey_time_temp
                                      set additional_time = avg_time_at_port + avg_journey_time;
                                      
                                      
                                      UPDATE ",input_schema,".avg_journey_time_temp
                                      set profiled_eta_to_ph = (case 
                                      when ais_destination='PORT HEDLAND' then ais_voyage_eta_f 
                                      else (ais_voyage_eta_f + additional_time * INTERVAL '1 hour') 
                                      end);
                                      
                                      
                                      ALTER TABLE ",input_schema,".avg_journey_time_temp  add column profiled_s5_eta_to_ph timestamp;
                                      
                                      -- Adding Profiled s5 eta
                                      UPDATE ",input_schema,".avg_journey_time_temp
                                      set profiled_s5_eta_to_ph = (case 
                                      when ais_destination='PORT HEDLAND' then s5_eta 
                                      else (s5_eta + additional_time * INTERVAL '1 hour') 
                                      end);
                                      
                                      -- Adding max and min eta
                                      ALTER TABLE ",input_schema,".avg_journey_time_temp add column max_eta timestamp;
                                      ALTER TABLE ",input_schema,".avg_journey_time_temp add column min_eta timestamp;
                                      
                                      UPDATE ",input_schema,".avg_journey_time_temp 
                                      set max_eta = greatest(vl_vessel_eta,fixed_eta_to_ph,profiled_eta_to_ph, profiled_s5_eta_to_ph);
                                      
                                      UPDATE ",input_schema,".avg_journey_time_temp 
                                      set min_eta = least(vl_vessel_eta,fixed_eta_to_ph,profiled_eta_to_ph, profiled_s5_eta_to_ph);
                                      
                                      ALTER TABLE ",input_schema,".avg_journey_time_temp add column three_eta_null_flag integer;
                                      
                                      -- Check to see if any three out of the 4 eta's are null
                                      UPDATE ",input_schema,".avg_journey_time_temp 
                                      set three_eta_null_flag = (CASE 
                                      WHEN vl_vessel_eta IS NOT NULL AND fixed_eta_to_ph IS NULL AND profiled_eta_to_ph IS NULL AND profiled_s5_eta_to_ph IS NULL THEN 1
                                      WHEN fixed_eta_to_ph IS NOT NULL AND vl_vessel_eta IS NULL AND profiled_eta_to_ph IS NULL AND profiled_s5_eta_to_ph IS NULL THEN 1
                                      WHEN profiled_eta_to_ph IS NOT NULL AND fixed_eta_to_ph IS NULL AND vl_vessel_eta IS NULL AND profiled_s5_eta_to_ph IS NULL THEN 1            
                                      WHEN profiled_s5_eta_to_ph IS NOT NULL AND fixed_eta_to_ph IS NULL AND profiled_eta_to_ph IS NULL AND vl_vessel_eta IS NULL THEN 1      
                                      ELSE 0
                                      END);         
                                      
                                      
                                      
                                      INSERT INTO ",input_schema,".ais_eta_standardization_daily
                                      SELECT DISTINCT *
                                      FROM ",input_schema,".avg_journey_time_temp;
                                      
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".ais_eta_standardization_daily_spotfire_temp;
                                      
                                      -- Removing of unnecessary records by sorting and ranking, before upload to spotfire. 
                                      CREATE TABLE ",input_schema,".ais_eta_standardization_daily_spotfire_temp AS
                                      WITH temp_ais_standard AS
                                      (SELECT DISTINCT min_eta, 
                                      max_eta, 
                                      ais_vl_fsr_vsr_date_position, 
                                      ais_vl_fsr_vsr_imo, 
                                      ais_vl_fsr_vsr_vessel_name, --ais_static_name, 
                                      ais_destination, 
                                      ais_shiptype, 
                                      fixed_eta_to_ph, 
                                      lane, 
                                      next_port, 
                                      profiled_eta_to_ph, 
                                      vl_vessel_eta, 
                                      vl_voyage_operator_name, 
                                      ais_vl_fsr_vsr_commodity, 
                                      ais_vl_fsr_vsr_incoterms,
                                      flag_bhp, 
                                      profiled_s5_eta_to_ph, 
                                      vl_fsr_vsr_voyage_number, 
                                      vl_fsr_vsr_nomination_number, 
                                      three_eta_null_flag,
                                      vl_vendor_name,
                                      vl_fsr_vessel_eta,
                                      DENSE_RANK() OVER (PARTITION BY ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position
                                      ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_date_position ASC, min_eta ASC, max_eta ASC) as rank
                                      FROM ",input_schema,".avg_journey_time_temp)
                                      SELECT *
                                      FROM temp_ais_standard
                                      WHERE rank = 1;
                                      
                                      
                                      INSERT INTO ",input_schema,".ais_eta_standardization_daily_spotfire
                                      SELECT DISTINCT *
                                      FROM ",input_schema,".ais_eta_standardization_daily_spotfire_temp;
                                      
                                      
                                      VACUUM ",input_schema,".ais_eta_standardization_daily_spotfire;"), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('ais_eta_standardization_daily_spotfire', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    #sys.sleep(10)
    
    break;
    
  }
  
  
  
  
  while (TRUE) {
    
    #********************************** Filename: 9. ETA Compliance to Plan Calculations 20171016.sql
    # print("9. ETA Compliance to Plan Calculations 20171016.sql")
    print(paste0("9. ETA Compliance to Plan Calculations ",today_date,".SQL"))
    
    # Description: Code to create the dataset and do the calculations necessary to drive the ETA Compliance to Plan view. 
    # Please note - All calculations are being done in the temp table and being converted to the final table in line 1054
    
    #################################### Section 14: ETA Compliance to Plan
    
    # Channel is opened
    channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
    
    # Get the required columns for the eta compliance to plan view
    src_data_name <- 'ais_eta_standardization_daily_spotfire'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("/*Getting the relevant columns for this visualization. This is a temp table.*/
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".eta_deviation_calc_daily_temp;
                                      CREATE TABLE ",input_schema,".eta_deviation_calc_daily_temp AS
                                      SELECT DISTINCT ais_vl_fsr_vsr_imo, 
                                      vl_fsr_vsr_nomination_number, 
                                      ais_vl_fsr_vsr_date_position, 
                                      ais_vl_fsr_vsr_vessel_name, --ais_static_name, 
                                      vl_fsr_vessel_eta,
                                      profiled_eta_to_ph, 
                                      fixed_eta_to_ph, 
                                      profiled_s5_eta_to_ph, 
                                      ais_vl_fsr_vsr_commodity, 
                                      ais_destination, 
                                      vl_voyage_operator_name, 
                                      ais_shiptype,
                                      ais_vl_fsr_vsr_incoterms,
                                      vl_fsr_vsr_voyage_number,
                                      vl_vendor_name
                                      FROM ",input_schema,".ais_eta_standardization_daily_spotfire
                                      WHERE flag_bhp LIKE 'BHP-%'
                                      AND ais_vl_fsr_vsr_date_position >= DATEADD(day, -25, '",today_date_formatted,"') ---enter today's date
                                      AND ais_vl_fsr_vsr_date_position <= '",today_date_formatted,"';
                                      
                                      INSERT INTO ",input_schema,".eta_deviation_calc_daily_temp
                                      SELECT DISTINCT ais_vl_fsr_vsr_imo, 
                                      vl_fsr_vsr_nomination_number, 
                                      ais_vl_fsr_vsr_date_position, 
                                      ais_vl_fsr_vsr_vessel_name, --ais_static_name, 
                                      vl_fsr_vessel_eta,
                                      profiled_eta_to_ph, 
                                      fixed_eta_to_ph, 
                                      profiled_s5_eta_to_ph, 
                                      ais_vl_fsr_vsr_commodity, 
                                      ais_destination, 
                                      vl_voyage_operator_name, 
                                      ais_shiptype,
                                      ais_vl_fsr_vsr_incoterms,
                                      vl_fsr_vsr_voyage_number,
                                      vl_vendor_name
                                      FROM ",input_schema,".ais_eta_standardization_daily_spotfire
                                      WHERE UPPER(flag_bhp) LIKE '%NON%'
                                      AND ais_vl_fsr_vsr_imo IN (SELECT DISTINCT ais_vl_fsr_vsr_imo
                                      FROM ",input_schema,".ais_vl_fsr_vsr_daily_spotfire
                                      WHERE flag_bhp LIKE 'BHP-%')
                                      AND ais_vl_fsr_vsr_date_position >= DATEADD(day, -25, '",today_date_formatted,"') ---enter today's date
                                      AND ais_vl_fsr_vsr_date_position <= '",today_date_formatted,"';
                                      
                                      
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_fsr_vsr_nomination_number_raw VARCHAR(250);
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_fsr_vsr_voyage_number_raw VARCHAR(250);
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET vl_fsr_vsr_nomination_number_raw = vl_fsr_vsr_nomination_number;
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET vl_fsr_vsr_voyage_number_raw = vl_fsr_vsr_voyage_number;"), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    # Calculation deviations for vessel list eta, 7 days ago
    src_data_name <- 'eta_deviation_calc_daily_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("-- Step 1 - Calculations for Vessel List data
                                      
                                      -- Step 1.1 - Calculations for 7 day deviation
                                      
                                      -- Adding necessary columns
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_date_7days_ago DATE default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_eta_7days_ago DATETIME default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_deviation_Hour_7Days_ATA INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_currentEtaLess_NextDay_7days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_currentEtaGreater_CurrentDay_7days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_pastEtaGreater_PastDay_7days INT default NULL;
                                      
                                      /* takes each ais_vl_fsr_vsr_date_position and subtract 7 days */
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET vl_date_7days_ago = DATEADD(day, -7, ais_vl_fsr_vsr_date_position);
                                      
                                      
                                      -- Checking for actual time of arrival using the condition ETA greater than current date and less then or equal to next date
                                      /*Ensures that current eta is greater than current date*/
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET vl_currentEtaGreater_CurrentDay_7days = (case 
                                      when TRUNC(vl_fsr_vessel_eta)::date >= ais_vl_fsr_vsr_date_position 
                                      then 1 else 0 
                                      end);
                                      
                                      /*Ensures that current eta is less than or equal to next day*/
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET vl_currentEtaLess_NextDay_7days = (case 
                                      when TRUNC(vl_fsr_vessel_eta)::date < DATEADD(day, 1, ais_vl_fsr_vsr_date_position)
                                      then 1 else 0 
                                      end);
                                      
                                      
                                      -- Vessel List ETA 7 Days Ago
                                      /* this makes sure that the vessel eta is greater than current day and less than or equal to next day. This eta will be the ata which will be used
                                      in the deviations calculation*/
                                      
                                      
                                      CREATE TABLE ",input_schema,".temp_etaCalc_7days AS
                                      SELECT DISTINCT e1.ais_vl_fsr_vsr_imo, e1.ais_vl_fsr_vsr_date_position, e1.vl_fsr_vessel_eta, e1.vl_date_7days_ago, ais_vl_fsr_vsr_incoterms, 
                                      e1.vl_fsr_vsr_voyage_number as unique_nominationOrVoyage_number
                                      FROM ",input_schema,".eta_deviation_calc_daily_temp e1 
                                      WHERE vl_fsr_vessel_eta IS NOT NULL;
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET vl_eta_7days_ago = ve.vl_fsr_vessel_eta
                                      FROM
                                      (SELECT et1.ais_vl_fsr_vsr_imo, et1.ais_vl_fsr_vsr_date_position, et1.unique_nominationOrVoyage_number, et1.vl_date_7days_ago, et2.vl_fsr_vessel_eta
                                      FROM ",input_schema,".temp_etaCalc_7days et1, ",input_schema,".temp_etaCalc_7days et2
                                      WHERE et1.vl_date_7days_ago = et2.ais_vl_fsr_vsr_date_position
                                      AND et1.ais_vl_fsr_vsr_imo = et2.ais_vl_fsr_vsr_imo
                                      AND et1.ais_vl_fsr_vsr_imo IS NOT NULL) as ve
                                      WHERE eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_date_position = ve.ais_vl_fsr_vsr_date_position
                                      AND eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_imo = ve.ais_vl_fsr_vsr_imo;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".temp_etaCalc_7days;
                                      
                                      /*Ensures that past eta is greater than past date*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET vl_pastEtaGreater_PastDay_7days = (case
                                      when TRUNC(vl_eta_7days_ago)::date >= vl_date_7days_ago 
                                      then 1 else 0 end);
                                      -- deviation hours ATA - ETA Vessel List
                                      /* Takes ETA Vessel List - ETA Vessel List 7 days ago*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET vl_deviation_Hour_7Days_ATA = (EXTRACT(HOUR FROM (vl_fsr_vessel_eta-vl_eta_7days_ago)))
                                      WHERE vl_currentEtaLess_NextDay_7days = 1
                                      AND vl_currentEtaGreater_CurrentDay_7days = 1
                                      AND vl_pastEtaGreater_PastDay_7days = 1;"), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    # Calculation deviations for vessel list eta, 14 days ago
    src_data_name <- 'eta_deviation_calc_daily_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("-- Step 1.2 - Calculations for 14 day deviation
                                      
                                      
                                      -- Adding columns _vl_14days
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_date_14days_ago DATE default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_eta_14days_ago DATETIME default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_deviation_Hour_14Days_ATA INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_currentEtaLess_NextDay_14days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_currentEtaGreater_CurrentDay_14days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_pastEtaGreater_PastDay_14days INT default NULL;
                                      
                                      -- adding 14 days ago _VL
                                      /* takes each ais_vl_fsr_vsr_date_position and subtract 14 days */
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET vl_date_14days_ago = DATEADD(day, -14, ais_vl_fsr_vsr_date_position);
                                      
                                      /*Ensures that current eta is greater than current date*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET vl_currentEtaGreater_CurrentDay_14days = (case 
                                      when TRUNC(vl_fsr_vessel_eta)::date >= ais_vl_fsr_vsr_date_position 
                                      then 1 else 0 
                                      end);
                                      
                                      /*Ensures that current eta is less than or equal to next day*/                                    
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET vl_currentEtaLess_NextDay_14days = (case 
                                      when TRUNC(vl_fsr_vessel_eta)::date < DATEADD(day, 1, ais_vl_fsr_vsr_date_position)
                                      then 1 else 0 
                                      end);
                                      
                                      
                                      -- Vessel List ETA 14 Days Ago
                                      /* this makes sure that the vessel eta is greater than current day and less than or equal to next day. This eta will be the ata which will be used
                                      in the deviations calculation*/
                                      
                                      CREATE TABLE ",input_schema,".temp_etaCalc_14days AS
                                      SELECT DISTINCT e1.ais_vl_fsr_vsr_imo, e1.ais_vl_fsr_vsr_date_position, e1.vl_fsr_vessel_eta, e1.vl_date_14days_ago, ais_vl_fsr_vsr_incoterms, 
                                      e1.vl_fsr_vsr_voyage_number as unique_nominationOrVoyage_number
                                      FROM ",input_schema,".eta_deviation_calc_daily_temp e1 
                                      WHERE vl_fsr_vessel_eta IS NOT NULL;
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET vl_eta_14days_ago = ve.vl_fsr_vessel_eta
                                      FROM
                                      (SELECT et1.ais_vl_fsr_vsr_imo, et1.ais_vl_fsr_vsr_date_position, et1.unique_nominationOrVoyage_number, et1.vl_date_14days_ago, et2.vl_fsr_vessel_eta
                                      FROM ",input_schema,".temp_etaCalc_14days et1, ",input_schema,".temp_etaCalc_14days et2
                                      WHERE et1.vl_date_14days_ago = et2.ais_vl_fsr_vsr_date_position
                                      AND et1.ais_vl_fsr_vsr_imo = et2.ais_vl_fsr_vsr_imo
                                      AND et1.ais_vl_fsr_vsr_imo IS NOT NULL) as ve
                                      WHERE eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_date_position = ve.ais_vl_fsr_vsr_date_position
                                      AND eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_imo = ve.ais_vl_fsr_vsr_imo;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".temp_etaCalc_14days;
                                      
                                      
                                      /*Ensures that past eta is greater than past date*/
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET vl_pastEtaGreater_PastDay_14days = (case
                                      when TRUNC(vl_eta_14days_ago)::date >= vl_date_14days_ago 
                                      then 1 else 0 end);
                                      
                                      -- deviation hours ATA - ETA Vessel List
                                      /* Takes ETA Vessel List - ETA Vessel List 14 days ago*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET vl_deviation_Hour_14Days_ATA = (EXTRACT(HOUR FROM (vl_fsr_vessel_eta-vl_eta_14days_ago)))
                                      WHERE vl_currentEtaLess_NextDay_14days = 1
                                      AND vl_currentEtaGreater_CurrentDay_14days = 1
                                      AND vl_pastEtaGreater_PastDay_14days = 1;
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    # Calculation deviations for vessel list eta, 21 days ago
    src_data_name <- 'eta_deviation_calc_daily_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("
                                      -- Step 1.3 - Calculations for 21 day deviation
                                      
                                      
                                      -- Adding columns _vl_21days
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_date_21days_ago DATE default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_eta_21days_ago DATETIME default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_deviation_Hour_21Days_ATA INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_currentEtaLess_NextDay_21days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_currentEtaGreater_CurrentDay_21days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN vl_pastEtaGreater_PastDay_21days INT default NULL;
                                      
                                      -- adding 21 days ago _VL
                                      /* takes each ais_vl_fsr_vsr_date_position and subtract 21 days */
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET vl_date_21days_ago = DATEADD(day, -21, ais_vl_fsr_vsr_date_position);
                                      
                                      /*Ensures that current eta is greater than current date*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET vl_currentEtaGreater_CurrentDay_21days = (case 
                                      when TRUNC(vl_fsr_vessel_eta)::date >= ais_vl_fsr_vsr_date_position 
                                      then 1 else 0 
                                      end);
                                      
                                      /*Ensures that current eta is less than or equal to next day*/                                   
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET vl_currentEtaLess_NextDay_21days = (case 
                                      when TRUNC(vl_fsr_vessel_eta)::date < DATEADD(day, 1, ais_vl_fsr_vsr_date_position)
                                      then 1 else 0 
                                      end);
                                      
                                      
                                      -- Vessel List ETA 21 Days Ago
                                      /* this makes sure that the vessel eta is greater than current day and less than or equal to next day. This eta will be the ata which will be used
                                      in the deviations calculation*/
                                      
                                      CREATE TABLE ",input_schema,".temp_etaCalc_21days AS
                                      SELECT DISTINCT e1.ais_vl_fsr_vsr_imo, e1.ais_vl_fsr_vsr_date_position, e1.vl_fsr_vessel_eta, e1.vl_date_21days_ago, ais_vl_fsr_vsr_incoterms, 
                                      e1.vl_fsr_vsr_voyage_number as unique_nominationOrVoyage_number
                                      FROM ",input_schema,".eta_deviation_calc_daily_temp e1 
                                      WHERE vl_fsr_vessel_eta IS NOT NULL;
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET vl_eta_21days_ago = ve.vl_fsr_vessel_eta
                                      FROM
                                      (SELECT et1.ais_vl_fsr_vsr_imo, et1.ais_vl_fsr_vsr_date_position, et1.unique_nominationOrVoyage_number, et1.vl_date_21days_ago, et2.vl_fsr_vessel_eta
                                      FROM ",input_schema,".temp_etaCalc_21days et1, ",input_schema,".temp_etaCalc_21days et2
                                      WHERE et1.vl_date_21days_ago = et2.ais_vl_fsr_vsr_date_position
                                      AND et1.ais_vl_fsr_vsr_imo = et2.ais_vl_fsr_vsr_imo
                                      AND et1.ais_vl_fsr_vsr_imo IS NOT NULL) as ve
                                      WHERE eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_date_position = ve.ais_vl_fsr_vsr_date_position
                                      AND eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_imo = ve.ais_vl_fsr_vsr_imo;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".temp_etaCalc_21days;
                                      
                                      
                                      /*Ensures that past eta is greater than past date*/
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET vl_pastEtaGreater_PastDay_21days = (case
                                      when TRUNC(vl_eta_21days_ago)::date >= vl_date_21days_ago 
                                      then 1 else 0 end);
                                      
                                      -- deviation hours ATA - ETA Vessel List
                                      /* Takes ETA Vessel List - ETA Vessel List 21 days ago*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET vl_deviation_Hour_21Days_ATA = (EXTRACT(HOUR FROM (vl_fsr_vessel_eta-vl_eta_21days_ago)))
                                      WHERE vl_currentEtaLess_NextDay_21days = 1
                                      AND vl_currentEtaGreater_CurrentDay_21days = 1
                                      AND vl_pastEtaGreater_PastDay_21days = 1;
                                      
                                      
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    
    # Calculation deviations for AIS profiled eta, 7 days ago
    src_data_name <- 'eta_deviation_calc_daily_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <-  sqlQuery(channel, paste0("-- Step 2 - Calculations for AIS Profiled ETA
                                       
                                       -- Step 2.1 - Calculations for 7 day deviation
                                       
                                       -- Adding columns _profiled_7days
                                       ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_date_7days_ago DATE default NULL;
                                       ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_eta_7days_ago DATETIME default NULL;
                                       ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_deviation_Hour_7Days_ATA INT default NULL;
                                       ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_currentEtaLess_NextDay_7days INT default NULL;
                                       ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_currentEtaGreater_CurrentDay_7days INT default NULL;
                                       ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_pastEtaGreater_PastDay_7days INT default NULL;
                                       
                                       -- adding 7 days ago _VL
                                       /* takes each ais_vl_fsr_vsr_date_position and subtract 7 days */
                                       UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                       SET profiled_date_7days_ago = DATEADD(day, -7, ais_vl_fsr_vsr_date_position);
                                       
                                       /*Ensures that current eta is greater than current date*/
                                       
                                       UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                       SET profiled_currentEtaGreater_CurrentDay_7days = (case 
                                       when TRUNC(profiled_eta_to_ph)::date >= ais_vl_fsr_vsr_date_position 
                                       then 1 else 0 
                                       end);
                                       
                                       /*Ensures that current eta is less than or equal to next day*/                                    
                                       UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                       SET profiled_currentEtaLess_NextDay_7days = (case 
                                       when TRUNC(profiled_eta_to_ph)::date < DATEADD(day, 1, ais_vl_fsr_vsr_date_position)
                                       then 1 else 0 
                                       end);
                                       
                                       
                                       -- Profiled ETA 7 Days Ago
                                       /* this makes sure that the vessel eta is greater than current day and less than or equal to next day. This eta will be the ata which will be used
                                       in the deviations calculation*/
                                       
                                       
                                       CREATE TABLE ",input_schema,".temp_etaCalc_7days AS
                                       SELECT DISTINCT e1.ais_vl_fsr_vsr_imo, e1.ais_vl_fsr_vsr_date_position, e1.profiled_eta_to_ph, e1.profiled_date_7days_ago, ais_vl_fsr_vsr_incoterms, 
                                       e1.vl_fsr_vsr_voyage_number as unique_nominationOrVoyage_number
                                       FROM ",input_schema,".eta_deviation_calc_daily_temp e1 
                                       WHERE profiled_eta_to_ph IS NOT NULL;
                                       
                                       UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                       SET profiled_eta_7days_ago = ve.profiled_eta_to_ph
                                       FROM
                                       (SELECT et1.ais_vl_fsr_vsr_imo, et1.ais_vl_fsr_vsr_date_position, et1.unique_nominationOrVoyage_number, et1.profiled_date_7days_ago, et2.profiled_eta_to_ph
                                       FROM ",input_schema,".temp_etaCalc_7days et1, ",input_schema,".temp_etaCalc_7days et2
                                       WHERE et1.profiled_date_7days_ago = et2.ais_vl_fsr_vsr_date_position
                                       AND et1.ais_vl_fsr_vsr_imo = et2.ais_vl_fsr_vsr_imo
                                       AND et1.ais_vl_fsr_vsr_imo IS NOT NULL) as ve
                                       WHERE eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_date_position = ve.ais_vl_fsr_vsr_date_position
                                       AND eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_imo = ve.ais_vl_fsr_vsr_imo;
                                       
                                       DROP TABLE IF EXISTS ",input_schema,".temp_etaCalc_7days;
                                       
                                       
                                       /*Ensures that past eta is greater than past date*/
                                       UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                       SET profiled_pastEtaGreater_PastDay_7days = (case
                                       when TRUNC(profiled_eta_7days_ago)::date >= profiled_date_7days_ago 
                                       then 1 else 0 end);
                                       
                                       -- deviation hours ATA - ETA Profiled
                                       /* Takes ETA Profiled - ETA Profiled 7 days ago*/
                                       
                                       UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                       SET profiled_deviation_Hour_7Days_ATA = (EXTRACT(HOUR FROM (profiled_eta_to_ph-profiled_eta_7days_ago)))
                                       WHERE profiled_currentEtaLess_NextDay_7days = 1
                                       AND profiled_currentEtaGreater_CurrentDay_7days = 1
                                       AND profiled_pastEtaGreater_PastDay_7days = 1;
                                       
                                       
                                       "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    # Calculation deviations for AIS profiled eta, 14 days ago
    src_data_name <- 'eta_deviation_calc_daily_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("
                                      -- Step 2.2 - Calculations for 14 day deviation
                                      
                                      -- Adding columns _profiled_14days
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_date_14days_ago DATE default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_eta_14days_ago DATETIME default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_deviation_Hour_14Days_ATA INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_currentEtaLess_NextDay_14days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_currentEtaGreater_CurrentDay_14days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_pastEtaGreater_PastDay_14days INT default NULL;
                                      
                                      -- adding 14 days ago _VL
                                      /* takes each ais_vl_fsr_vsr_date_position and subtract 14 days */
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET profiled_date_14days_ago = DATEADD(day, -14, ais_vl_fsr_vsr_date_position);
                                      
                                      /*Ensures that current eta is greater than current date*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET profiled_currentEtaGreater_CurrentDay_14days = (case 
                                      when TRUNC(profiled_eta_to_ph)::date >= ais_vl_fsr_vsr_date_position 
                                      then 1 else 0 
                                      end);
                                      
                                      /*Ensures that current eta is less than or equal to next day*/                                   
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET profiled_currentEtaLess_NextDay_14days = (case 
                                      when TRUNC(profiled_eta_to_ph)::date < DATEADD(day, 1, ais_vl_fsr_vsr_date_position)
                                      then 1 else 0 
                                      end);
                                      
                                      
                                      -- Profiled ETA 14 Days Ago
                                      /* this makes sure that the vessel eta is greater than current day and less than or equal to next day. This eta will be the ata which will be used
                                      in the deviations calculation*/
                                      
                                      CREATE TABLE ",input_schema,".temp_etaCalc_14days AS
                                      SELECT DISTINCT e1.ais_vl_fsr_vsr_imo, e1.ais_vl_fsr_vsr_date_position, e1.profiled_eta_to_ph, e1.profiled_date_14days_ago, ais_vl_fsr_vsr_incoterms, 
                                      e1.vl_fsr_vsr_voyage_number as unique_nominationOrVoyage_number
                                      FROM ",input_schema,".eta_deviation_calc_daily_temp e1 
                                      WHERE profiled_eta_to_ph IS NOT NULL;
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET profiled_eta_14days_ago = ve.profiled_eta_to_ph
                                      FROM
                                      (SELECT et1.ais_vl_fsr_vsr_imo, et1.ais_vl_fsr_vsr_date_position, et1.unique_nominationOrVoyage_number, et1.profiled_date_14days_ago, et2.profiled_eta_to_ph
                                      FROM ",input_schema,".temp_etaCalc_14days et1, ",input_schema,".temp_etaCalc_14days et2
                                      WHERE et1.profiled_date_14days_ago = et2.ais_vl_fsr_vsr_date_position
                                      AND et1.ais_vl_fsr_vsr_imo = et2.ais_vl_fsr_vsr_imo
                                      AND et1.ais_vl_fsr_vsr_imo IS NOT NULL) as ve
                                      WHERE eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_date_position = ve.ais_vl_fsr_vsr_date_position
                                      AND eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_imo = ve.ais_vl_fsr_vsr_imo;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".temp_etaCalc_14days;
                                      
                                      
                                      /*Ensures that past eta is greater than past date*/
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET profiled_pastEtaGreater_PastDay_14days = (case
                                      when TRUNC(profiled_eta_14days_ago)::date >= profiled_date_14days_ago 
                                      then 1 else 0 end);
                                      
                                      -- deviation hours ATA - ETA Profiled
                                      /* Takes ETA Profiled - ETA Profiled 14 days ago*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET profiled_deviation_Hour_14Days_ATA = (EXTRACT(HOUR FROM (profiled_eta_to_ph-profiled_eta_14days_ago)))
                                      WHERE profiled_currentEtaLess_NextDay_14days = 1
                                      AND profiled_currentEtaGreater_CurrentDay_14days = 1
                                      AND profiled_pastEtaGreater_PastDay_14days = 1;
                                      
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    # Calculation deviations for AIS profiled eta, 21 days ago
    src_data_name <- 'eta_deviation_calc_daily_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("
                                      
                                      -- Step 2.3 - Calculations for 21 day deviation
                                      
                                      -- Adding columns _profiled_21days
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_date_21days_ago DATE default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_eta_21days_ago DATETIME default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_deviation_Hour_21Days_ATA INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_currentEtaLess_NextDay_21days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_currentEtaGreater_CurrentDay_21days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN profiled_pastEtaGreater_PastDay_21days INT default NULL;
                                      
                                      -- adding 21 days ago _VL
                                      /* takes each ais_vl_fsr_vsr_date_position and subtract 21 days */
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET profiled_date_21days_ago = DATEADD(day, -21, ais_vl_fsr_vsr_date_position);
                                      
                                      /*Ensures that current eta is greater than current date*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET profiled_currentEtaGreater_CurrentDay_21days = (case 
                                      when TRUNC(profiled_eta_to_ph)::date >= ais_vl_fsr_vsr_date_position 
                                      then 1 else 0 
                                      end);
                                      
                                      /*Ensures that current eta is less than or equal to next day*/                                    
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET profiled_currentEtaLess_NextDay_21days = (case 
                                      when TRUNC(profiled_eta_to_ph)::date < DATEADD(day, 1, ais_vl_fsr_vsr_date_position)
                                      then 1 else 0 
                                      end);
                                      
                                      
                                      -- Profiled ETA 21 Days Ago
                                      /* this makes sure that the vessel eta is greater than current day and less than or equal to next day. This eta will be the ata which will be used
                                      in the deviations calculation*/
                                      
                                      
                                      CREATE TABLE ",input_schema,".temp_etaCalc_21days AS
                                      SELECT DISTINCT e1.ais_vl_fsr_vsr_imo, e1.ais_vl_fsr_vsr_date_position, e1.profiled_eta_to_ph, e1.profiled_date_21days_ago, ais_vl_fsr_vsr_incoterms, 
                                      e1.vl_fsr_vsr_voyage_number as unique_nominationOrVoyage_number
                                      FROM ",input_schema,".eta_deviation_calc_daily_temp e1 
                                      WHERE profiled_eta_to_ph IS NOT NULL;
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET profiled_eta_21days_ago = ve.profiled_eta_to_ph
                                      FROM
                                      (SELECT et1.ais_vl_fsr_vsr_imo, et1.ais_vl_fsr_vsr_date_position, et1.unique_nominationOrVoyage_number, et1.profiled_date_21days_ago, et2.profiled_eta_to_ph
                                      FROM ",input_schema,".temp_etaCalc_21days et1, ",input_schema,".temp_etaCalc_21days et2
                                      WHERE et1.profiled_date_21days_ago = et2.ais_vl_fsr_vsr_date_position
                                      AND et1.ais_vl_fsr_vsr_imo = et2.ais_vl_fsr_vsr_imo
                                      AND et1.ais_vl_fsr_vsr_imo IS NOT NULL) as ve
                                      WHERE eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_date_position = ve.ais_vl_fsr_vsr_date_position
                                      AND eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_imo = ve.ais_vl_fsr_vsr_imo;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".temp_etaCalc_21days;
                                      
                                      
                                      /*Ensures that past eta is greater than past date*/
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET profiled_pastEtaGreater_PastDay_21days = (case
                                      when TRUNC(profiled_eta_21days_ago)::date >= profiled_date_21days_ago 
                                      then 1 else 0 end);
                                      
                                      -- deviation hours ATA - ETA Profiled
                                      /* Takes ETA Profiled - ETA Profiled 21 days ago*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET profiled_deviation_Hour_21Days_ATA = (EXTRACT(HOUR FROM (profiled_eta_to_ph-profiled_eta_21days_ago)))
                                      WHERE profiled_currentEtaLess_NextDay_21days = 1
                                      AND profiled_currentEtaGreater_CurrentDay_21days = 1
                                      AND profiled_pastEtaGreater_PastDay_21days = 1;"), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    # Calculation deviations for AIS operator eta, 7 days ago
    src_data_name <- 'eta_deviation_calc_daily_temp_ais_7'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("
                                      
                                      -- Step 3 - Calculations for AIS Operator ETA (referring to it as 'fixed eta' as it's reliant on distance table)
                                      
                                      -- Step 3.1 - Calculations for 7 day deviation
                                      
                                      
                                      -- Adding columns _fixed_7days
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_date_7days_ago DATE default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_eta_7days_ago DATETIME default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_deviation_Hour_7Days_ATA INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_currentEtaLess_NextDay_7days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_currentEtaGreater_CurrentDay_7days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_pastEtaGreater_PastDay_7days INT default NULL;
                                      
                                      -- adding 7 days ago _VL
                                      /* takes each ais_vl_fsr_vsr_date_position and subtract 7 days */
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET fixed_date_7days_ago = DATEADD(day, -7, ais_vl_fsr_vsr_date_position);
                                      
                                      /*Ensures that current eta is greater than current date*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET fixed_currentEtaGreater_CurrentDay_7days = (case 
                                      when TRUNC(fixed_eta_to_ph)::date >= ais_vl_fsr_vsr_date_position 
                                      then 1 else 0 
                                      end);
                                      
                                      /*Ensures that current eta is less than or equal to next day*/                                    
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET fixed_currentEtaLess_NextDay_7days = (case 
                                      when TRUNC(fixed_eta_to_ph)::date < DATEADD(day, 1, ais_vl_fsr_vsr_date_position)
                                      then 1 else 0 
                                      end);
                                      
                                      
                                      -- Operator ETA 7 Days Ago
                                      /* this makes sure that the vessel eta is greater than current day and less than or equal to next day. This eta will be the ata which will be used
                                      in the deviations calculation*/
                                      
                                      CREATE TABLE ",input_schema,".temp_etaCalc_7days AS
                                      SELECT DISTINCT e1.ais_vl_fsr_vsr_imo, e1.ais_vl_fsr_vsr_date_position, e1.fixed_eta_to_ph, e1.fixed_date_7days_ago, ais_vl_fsr_vsr_incoterms, 
                                      e1.vl_fsr_vsr_voyage_number as unique_nominationOrVoyage_number
                                      FROM ",input_schema,".eta_deviation_calc_daily_temp e1 
                                      WHERE fixed_eta_to_ph IS NOT NULL;
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET fixed_eta_7days_ago = ve.fixed_eta_to_ph
                                      FROM
                                      (SELECT et1.ais_vl_fsr_vsr_imo, et1.ais_vl_fsr_vsr_date_position, et1.unique_nominationOrVoyage_number, et1.fixed_date_7days_ago, et2.fixed_eta_to_ph
                                      FROM ",input_schema,".temp_etaCalc_7days et1, ",input_schema,".temp_etaCalc_7days et2
                                      WHERE et1.fixed_date_7days_ago = et2.ais_vl_fsr_vsr_date_position
                                      AND et1.ais_vl_fsr_vsr_imo = et2.ais_vl_fsr_vsr_imo
                                      AND et1.ais_vl_fsr_vsr_imo IS NOT NULL) as ve
                                      WHERE eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_date_position = ve.ais_vl_fsr_vsr_date_position
                                      AND eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_imo = ve.ais_vl_fsr_vsr_imo;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".temp_etaCalc_7days;
                                      
                                      
                                      /*Ensures that past eta is greater than past date*/
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET fixed_pastEtaGreater_PastDay_7days = (case
                                      when TRUNC(fixed_eta_7days_ago)::date >= fixed_date_7days_ago 
                                      then 1 else 0 end);
                                      
                                      -- deviation hours ATA - ETA Operator
                                      /* Takes ETA Operator - ETA Operator 7 days ago*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET fixed_deviation_Hour_7Days_ATA = (EXTRACT(HOUR FROM (fixed_eta_to_ph-fixed_eta_7days_ago)))
                                      WHERE fixed_currentEtaLess_NextDay_7days = 1
                                      AND fixed_currentEtaGreater_CurrentDay_7days = 1
                                      AND fixed_pastEtaGreater_PastDay_7days = 1;"), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    # Calculation deviations for AIS operator eta, 14 days ago
    src_data_name <- 'eta_deviation_calc_daily_temp_ais_14'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("
                                      -- Step 3.2 - Calculations for 14 day deviation
                                      
                                      --  Adding columns _fixed_14days
                                      
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_date_14days_ago DATE default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_eta_14days_ago DATETIME default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_deviation_Hour_14Days_ATA INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_currentEtaLess_NextDay_14days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_currentEtaGreater_CurrentDay_14days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_pastEtaGreater_PastDay_14days INT default NULL;
                                      
                                      -- adding 14 days ago _VL
                                      /* takes each ais_vl_fsr_vsr_date_position and subtract 14 days */
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET fixed_date_14days_ago = DATEADD(day, -14, ais_vl_fsr_vsr_date_position);
                                      
                                      /*Ensures that current eta is greater than current date*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET fixed_currentEtaGreater_CurrentDay_14days = (case 
                                      when TRUNC(fixed_eta_to_ph)::date >= ais_vl_fsr_vsr_date_position 
                                      then 1 else 0 
                                      end);
                                      
                                      /*Ensures that current eta is less than or equal to next day*/                                    
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET fixed_currentEtaLess_NextDay_14days = (case 
                                      when TRUNC(fixed_eta_to_ph)::date < DATEADD(day, 1, ais_vl_fsr_vsr_date_position)
                                      then 1 else 0 
                                      end);
                                      
                                      
                                      -- Operator ETA 14 Days Ago
                                      /* this makes sure that the vessel eta is greater than current day and less than or equal to next day. This eta will be the ata which will be used
                                      in the deviations calculation*/
                                      
                                      CREATE TABLE ",input_schema,".temp_etaCalc_14days AS
                                      SELECT DISTINCT e1.ais_vl_fsr_vsr_imo, e1.ais_vl_fsr_vsr_date_position, e1.fixed_eta_to_ph, e1.fixed_date_14days_ago, ais_vl_fsr_vsr_incoterms, 
                                      e1.vl_fsr_vsr_voyage_number as unique_nominationOrVoyage_number
                                      FROM ",input_schema,".eta_deviation_calc_daily_temp e1 
                                      WHERE fixed_eta_to_ph IS NOT NULL;
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET fixed_eta_14days_ago = ve.fixed_eta_to_ph
                                      FROM
                                      (SELECT et1.ais_vl_fsr_vsr_imo, et1.ais_vl_fsr_vsr_date_position, et1.unique_nominationOrVoyage_number, et1.fixed_date_14days_ago, et2.fixed_eta_to_ph
                                      FROM ",input_schema,".temp_etaCalc_14days et1, ",input_schema,".temp_etaCalc_14days et2
                                      WHERE et1.fixed_date_14days_ago = et2.ais_vl_fsr_vsr_date_position
                                      AND et1.ais_vl_fsr_vsr_imo = et2.ais_vl_fsr_vsr_imo
                                      AND et1.ais_vl_fsr_vsr_imo IS NOT NULL) as ve
                                      WHERE eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_date_position = ve.ais_vl_fsr_vsr_date_position
                                      AND eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_imo = ve.ais_vl_fsr_vsr_imo;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".temp_etaCalc_14days;
                                      
                                      
                                      /*Ensures that past eta is greater than past date*/
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET fixed_pastEtaGreater_PastDay_14days = (case
                                      when TRUNC(fixed_eta_14days_ago)::date >= fixed_date_14days_ago 
                                      then 1 else 0 end);
                                      
                                      -- deviation hours ATA - ETA Operator
                                      /* Takes ETA Operator - ETA Operator 14 days ago*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET fixed_deviation_Hour_14Days_ATA = (EXTRACT(HOUR FROM (fixed_eta_to_ph-fixed_eta_14days_ago)))
                                      WHERE fixed_currentEtaLess_NextDay_14days = 1
                                      AND fixed_currentEtaGreater_CurrentDay_14days = 1
                                      AND fixed_pastEtaGreater_PastDay_14days = 1;"), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    # Calculation deviations for AIS operator eta, 21 days ago
    src_data_name <- 'eta_deviation_calc_daily_temp_ais_21'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("
                                      -- Step 3.3 - Calculations for 21 day deviation
                                      
                                      -- Adding columns _fixed_21days
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_date_21days_ago DATE default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_eta_21days_ago DATETIME default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_deviation_Hour_21Days_ATA INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_currentEtaLess_NextDay_21days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_currentEtaGreater_CurrentDay_21days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN fixed_pastEtaGreater_PastDay_21days INT default NULL;
                                      
                                      -- adding 21 days ago _VL
                                      /* takes each ais_vl_fsr_vsr_date_position and subtract 21 days */
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET fixed_date_21days_ago = DATEADD(day, -21, ais_vl_fsr_vsr_date_position);
                                      
                                      /*Ensures that current eta is greater than current date*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET fixed_currentEtaGreater_CurrentDay_21days = (case 
                                      when TRUNC(fixed_eta_to_ph)::date >= ais_vl_fsr_vsr_date_position 
                                      then 1 else 0 
                                      end);
                                      
                                      /*Ensures that current eta is less than or equal to next day*/                                   
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET fixed_currentEtaLess_NextDay_21days = (case 
                                      when TRUNC(fixed_eta_to_ph)::date < DATEADD(day, 1, ais_vl_fsr_vsr_date_position)
                                      then 1 else 0 
                                      end);
                                      
                                      
                                      -- -Operator ETA 21 Days Ago
                                      /* this makes sure that the vessel eta is greater than current day and less than or equal to next day. This eta will be the ata which will be used
                                      in the deviations calculation*/
                                      
                                      CREATE TABLE ",input_schema,".temp_etaCalc_21days AS
                                      SELECT DISTINCT e1.ais_vl_fsr_vsr_imo, e1.ais_vl_fsr_vsr_date_position, e1.fixed_eta_to_ph, e1.fixed_date_21days_ago, ais_vl_fsr_vsr_incoterms, 
                                      e1.vl_fsr_vsr_voyage_number as unique_nominationOrVoyage_number
                                      FROM ",input_schema,".eta_deviation_calc_daily_temp e1 
                                      WHERE fixed_eta_to_ph IS NOT NULL;
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET fixed_eta_21days_ago = ve.fixed_eta_to_ph
                                      FROM
                                      (SELECT et1.ais_vl_fsr_vsr_imo, et1.ais_vl_fsr_vsr_date_position, et1.unique_nominationOrVoyage_number, et1.fixed_date_21days_ago, et2.fixed_eta_to_ph
                                      FROM ",input_schema,".temp_etaCalc_21days et1, ",input_schema,".temp_etaCalc_21days et2
                                      WHERE et1.fixed_date_21days_ago = et2.ais_vl_fsr_vsr_date_position
                                      AND et1.ais_vl_fsr_vsr_imo = et2.ais_vl_fsr_vsr_imo
                                      AND et1.ais_vl_fsr_vsr_imo IS NOT NULL) as ve
                                      WHERE eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_date_position = ve.ais_vl_fsr_vsr_date_position
                                      AND eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_imo = ve.ais_vl_fsr_vsr_imo;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".temp_etaCalc_21days;
                                      
                                      /*Ensures that past eta is greater than past date*/
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET fixed_pastEtaGreater_PastDay_21days = (case
                                      when TRUNC(fixed_eta_21days_ago)::date >= fixed_date_21days_ago 
                                      then 1 else 0 end);
                                      
                                      -- deviation hours ATA - ETA Operator
                                      /* Takes ETA Operator - ETA Operator 21 days ago*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET fixed_deviation_Hour_21Days_ATA = (EXTRACT(HOUR FROM (fixed_eta_to_ph-fixed_eta_21days_ago)))
                                      WHERE fixed_currentEtaLess_NextDay_21days = 1
                                      AND fixed_currentEtaGreater_CurrentDay_21days = 1
                                      AND fixed_pastEtaGreater_PastDay_21days = 1;"), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    # Calculation deviations for S5 eta, 7 days ago
    src_data_name <- 'eta_deviation_calc_daily_temp_s5_7'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("-- Step 4 - Calculations for S5 ETA
                                      
                                      -- Step 4.1 - Calculations for 7 day deviation
                                      
                                      -- Adding columns _s5_7days
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_date_7days_ago DATE default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_eta_7days_ago DATETIME default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_deviation_Hour_7Days_ATA INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_currentEtaLess_NextDay_7days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_currentEtaGreater_CurrentDay_7days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_pastEtaGreater_PastDay_7days INT default NULL;
                                      
                                      -- adding 7 days ago _VL
                                      /* takes each ais_vl_fsr_vsr_date_position and subtract 7 days */
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET s5_date_7days_ago = DATEADD(day, -7, ais_vl_fsr_vsr_date_position);
                                      
                                      /*Ensures that current eta is greater than current date*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET s5_currentEtaGreater_CurrentDay_7days = (case 
                                      when TRUNC(profiled_s5_eta_to_ph)::date >= ais_vl_fsr_vsr_date_position 
                                      then 1 else 0 
                                      end);
                                      
                                      /*Ensures that current eta is less than or equal to next day*/                                    
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET s5_currentEtaLess_NextDay_7days = (case 
                                      when TRUNC(profiled_s5_eta_to_ph)::date < DATEADD(day, 1, ais_vl_fsr_vsr_date_position)
                                      then 1 else 0 
                                      end);
                                      
                                      
                                      -- S5 ETA 7 Days Ago
                                      /* this makes sure that the vessel eta is greater than current day and less than or equal to next day. This eta will be the ata which will be used
                                      in the deviations calculation*/
                                      
                                      CREATE TABLE ",input_schema,".temp_etaCalc_7days AS 
                                      SELECT DISTINCT e1.ais_vl_fsr_vsr_imo, e1.ais_vl_fsr_vsr_date_position, e1.profiled_s5_eta_to_ph, e1.s5_date_7days_ago, ais_vl_fsr_vsr_incoterms, 
                                      e1.vl_fsr_vsr_voyage_number as unique_nominationOrVoyage_number
                                      FROM ",input_schema,".eta_deviation_calc_daily_temp e1 
                                      WHERE profiled_s5_eta_to_ph IS NOT NULL;
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET s5_eta_7days_ago = ve.profiled_s5_eta_to_ph
                                      FROM
                                      (SELECT et1.ais_vl_fsr_vsr_imo, et1.ais_vl_fsr_vsr_date_position, et1.unique_nominationOrVoyage_number, et1.s5_date_7days_ago, et2.profiled_s5_eta_to_ph
                                      FROM ",input_schema,".temp_etaCalc_7days et1, ",input_schema,".temp_etaCalc_7days et2
                                      WHERE et1.s5_date_7days_ago = et2.ais_vl_fsr_vsr_date_position
                                      AND et1.ais_vl_fsr_vsr_imo = et2.ais_vl_fsr_vsr_imo
                                      AND et1.ais_vl_fsr_vsr_imo IS NOT NULL) as ve
                                      WHERE eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_date_position = ve.ais_vl_fsr_vsr_date_position
                                      AND eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_imo = ve.ais_vl_fsr_vsr_imo;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".temp_etaCalc_7days;
                                      
                                      
                                      
                                      /*Ensures that past eta is greater than past date*/
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET s5_pastEtaGreater_PastDay_7days = (case
                                      when TRUNC(s5_eta_7days_ago)::date >= s5_date_7days_ago 
                                      then 1 else 0 end);
                                      
                                      ----------------------------------- deviation hours ATA - ETA S5
                                      /* Takes ETA S5 - ETA S5 7 days ago*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET s5_deviation_Hour_7Days_ATA = (EXTRACT(HOUR FROM (profiled_s5_eta_to_ph-s5_eta_7days_ago)))
                                      WHERE s5_currentEtaLess_NextDay_7days = 1
                                      AND s5_currentEtaGreater_CurrentDay_7days = 1
                                      AND s5_pastEtaGreater_PastDay_7days = 1;"), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    # Calculation deviations for S5 eta, 14 days ago
    src_data_name <- 'eta_deviation_calc_daily_temp_s5_14'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("
                                      
                                      -- Step 4.2 - Calculations for 14 day deviation
                                      
                                      -- Adding columns _s5_14days
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_date_14days_ago DATE default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_eta_14days_ago DATETIME default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_deviation_Hour_14Days_ATA INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_currentEtaLess_NextDay_14days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_currentEtaGreater_CurrentDay_14days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_pastEtaGreater_PastDay_14days INT default NULL;
                                      
                                      -- adding 14 days ago _VL
                                      /* takes each ais_vl_fsr_vsr_date_position and subtract 14 days */
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET s5_date_14days_ago = DATEADD(day, -14, ais_vl_fsr_vsr_date_position);
                                      
                                      /*Ensures that current eta is greater than current date*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET s5_currentEtaGreater_CurrentDay_14days = (case 
                                      when TRUNC(profiled_s5_eta_to_ph)::date >= ais_vl_fsr_vsr_date_position 
                                      then 1 else 0 
                                      end);
                                      
                                      /*Ensures that current eta is less than or equal to next day*/                                    
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET s5_currentEtaLess_NextDay_14days = (case 
                                      when TRUNC(profiled_s5_eta_to_ph)::date < DATEADD(day, 1, ais_vl_fsr_vsr_date_position)
                                      then 1 else 0 
                                      end);
                                      
                                      
                                      -- S5 ETA 14 Days Ago
                                      /* this makes sure that the vessel eta is greater than current day and less than or equal to next day. This eta will be the ata which will be used
                                      in the deviations calculation*/
                                      
                                      CREATE TABLE ",input_schema,".temp_etaCalc_14days AS
                                      SELECT DISTINCT e1.ais_vl_fsr_vsr_imo, e1.ais_vl_fsr_vsr_date_position, e1.profiled_s5_eta_to_ph, e1.s5_date_14days_ago, ais_vl_fsr_vsr_incoterms, 
                                      e1.vl_fsr_vsr_voyage_number as unique_nominationOrVoyage_number
                                      FROM ",input_schema,".eta_deviation_calc_daily_temp e1 
                                      WHERE profiled_s5_eta_to_ph IS NOT NULL;
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET s5_eta_14days_ago = ve.profiled_s5_eta_to_ph
                                      FROM
                                      (SELECT et1.ais_vl_fsr_vsr_imo, et1.ais_vl_fsr_vsr_date_position, et1.unique_nominationOrVoyage_number, et1.s5_date_14days_ago, et2.profiled_s5_eta_to_ph
                                      FROM ",input_schema,".temp_etaCalc_14days et1, ",input_schema,".temp_etaCalc_14days et2
                                      WHERE et1.s5_date_14days_ago = et2.ais_vl_fsr_vsr_date_position
                                      AND et1.ais_vl_fsr_vsr_imo = et2.ais_vl_fsr_vsr_imo
                                      AND et1.ais_vl_fsr_vsr_imo IS NOT NULL) as ve
                                      WHERE eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_date_position = ve.ais_vl_fsr_vsr_date_position
                                      AND eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_imo = ve.ais_vl_fsr_vsr_imo;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".temp_etaCalc_14days;
                                      
                                      
                                      /*Ensures that past eta is greater than past date*/
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET s5_pastEtaGreater_PastDay_14days = (case
                                      when TRUNC(s5_eta_14days_ago)::date >= s5_date_14days_ago 
                                      then 1 else 0 end);
                                      
                                      -- deviation hours ATA - ETA S5
                                      /* Takes ETA S5 - ETA S5 14 days ago*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET s5_deviation_Hour_14Days_ATA = (EXTRACT(HOUR FROM (profiled_s5_eta_to_ph-s5_eta_14days_ago)))
                                      WHERE s5_currentEtaLess_NextDay_14days = 1
                                      AND s5_currentEtaGreater_CurrentDay_14days = 1
                                      AND s5_pastEtaGreater_PastDay_14days = 1;"), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    # Calculation deviations for S5 eta, 21 days ago
    src_data_name <- 'eta_deviation_calc_daily_temp_s5_21'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("
                                      -- Step 4.3 - Calculations for 21 day deviation
                                      
                                      -- Adding columns _s5_21days
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_date_21days_ago DATE default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_eta_21days_ago DATETIME default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_deviation_Hour_21Days_ATA INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_currentEtaLess_NextDay_21days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_currentEtaGreater_CurrentDay_21days INT default NULL;
                                      ALTER TABLE ",input_schema,".eta_deviation_calc_daily_temp ADD COLUMN s5_pastEtaGreater_PastDay_21days INT default NULL;
                                      
                                      -- Adding 21 days ago _VL
                                      /* takes each ais_vl_fsr_vsr_date_position and subtract 21 days */
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET s5_date_21days_ago = DATEADD(day, -21, ais_vl_fsr_vsr_date_position);
                                      
                                      /*Ensures that current eta is greater than current date*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET s5_currentEtaGreater_CurrentDay_21days = (case 
                                      when TRUNC(profiled_s5_eta_to_ph)::date >= ais_vl_fsr_vsr_date_position 
                                      then 1 else 0 
                                      end);
                                      
                                      /*Ensures that current eta is less than or equal to next day*/                                    
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET s5_currentEtaLess_NextDay_21days = (case 
                                      when TRUNC(profiled_s5_eta_to_ph)::date < DATEADD(day, 1, ais_vl_fsr_vsr_date_position)
                                      then 1 else 0 
                                      end);
                                      
                                      
                                      -- S5 ETA 21 Days Ago
                                      /* this makes sure that the vessel eta is greater than current day and less than or equal to next day. This eta will be the ata which will be used
                                      in the deviations calculation*/
                                      
                                      CREATE TABLE ",input_schema,".temp_etaCalc_21days AS
                                      SELECT DISTINCT e1.ais_vl_fsr_vsr_imo, e1.ais_vl_fsr_vsr_date_position, e1.profiled_s5_eta_to_ph, e1.s5_date_21days_ago, ais_vl_fsr_vsr_incoterms, 
                                      e1.vl_fsr_vsr_voyage_number as unique_nominationOrVoyage_number
                                      FROM ",input_schema,".eta_deviation_calc_daily_temp e1 
                                      WHERE profiled_s5_eta_to_ph IS NOT NULL;
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET s5_eta_21days_ago = ve.profiled_s5_eta_to_ph
                                      FROM
                                      (SELECT et1.ais_vl_fsr_vsr_imo, et1.ais_vl_fsr_vsr_date_position, et1.unique_nominationOrVoyage_number, et1.s5_date_21days_ago, et2.profiled_s5_eta_to_ph
                                      FROM ",input_schema,".temp_etaCalc_21days et1, ",input_schema,".temp_etaCalc_21days et2
                                      WHERE et1.s5_date_21days_ago = et2.ais_vl_fsr_vsr_date_position
                                      AND et1.ais_vl_fsr_vsr_imo = et2.ais_vl_fsr_vsr_imo
                                      AND et1.ais_vl_fsr_vsr_imo IS NOT NULL) as ve
                                      WHERE eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_date_position = ve.ais_vl_fsr_vsr_date_position
                                      AND eta_deviation_calc_daily_temp.ais_vl_fsr_vsr_imo = ve.ais_vl_fsr_vsr_imo;
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".temp_etaCalc_21days;
                                      
                                      /*Ensures that past eta is greater than past date*/
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp 
                                      SET s5_pastEtaGreater_PastDay_21days = (case
                                      when TRUNC(s5_eta_21days_ago)::date >= s5_date_21days_ago 
                                      then 1 else 0 end);
                                      
                                      -- Deviation hours ATA - ETA S5
                                      /* Takes ETA S5 - ETA S5 21 days ago*/
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily_temp
                                      SET s5_deviation_Hour_21Days_ATA = (EXTRACT(HOUR FROM (profiled_s5_eta_to_ph-s5_eta_21days_ago)))
                                      WHERE s5_currentEtaLess_NextDay_21days = 1
                                      AND s5_currentEtaGreater_CurrentDay_21days = 1
                                      AND s5_pastEtaGreater_PastDay_21days = 1;
                                      
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    src_data_name <- 'eta_deviation_calc_daily_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("
                                      -- ***************************************************************************************************************************************************************
                                      
                                      -- Appending the historical eta deviations data to the current data and creating the table that will drive eta compliance to plan view.
                                      
                                      INSERT INTO ",input_schema,".eta_deviation_calc_daily
                                      SELECT *
                                      FROM ",input_schema,".eta_deviation_calc_daily_temp
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"'; ----------enter today's date "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    
    src_data_name <- 'eta_deviation_calc_daily_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("
                                      DROP TABLE IF EXISTS ",input_schema,".voy_nom_comb_temp;
                                      
                                      CREATE TABLE ",input_schema,".voy_nom_comb_temp AS
                                      SELECT DISTINCT vl_fsr_vsr_voyage_number, vl_fsr_vsr_nomination_number
                                      FROM ",input_schema,".ais_vl_fsr_vsr_daily
                                      WHERE ais_vl_fsr_vsr_date_position <= '",today_date_formatted,"'  --(enter today date)
                                      AND vl_fsr_vsr_voyage_number IS NOT NULL
                                      AND NOT (vl_fsr_vsr_nomination_number IS NULL or vl_fsr_vsr_nomination_number = '');
                                      
                                      
                                      
                                      
                                      -- if voyage has more than one nomination, then put as 'multiple nominations'
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily
                                      SET vl_fsr_vsr_nomination_number = '* MULTIPLE NOs.*'
                                      WHERE ais_vl_fsr_vsr_date_position = '",today_date_formatted,"' --(enter today date)
                                      AND (eta_deviation_calc_daily.vl_fsr_vsr_nomination_number IS NULL or eta_deviation_calc_daily.vl_fsr_vsr_nomination_number = '')
                                      AND eta_deviation_calc_daily.vl_fsr_vsr_voyage_number IN (SELECT vl_fsr_vsr_voyage_number
                                      FROM ",input_schema,".voy_nom_comb_temp
                                      GROUP BY vl_fsr_vsr_voyage_number
                                      HAVING COUNT(vl_fsr_vsr_nomination_number) > 1);"), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    src_data_name <- 'eta_deviation_calc_daily_temp'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("
                                      
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".voy_nom_comb_temp;
                                      
                                      CREATE TABLE ",input_schema,".voy_nom_comb_temp AS
                                      SELECT DISTINCT vl_fsr_vsr_voyage_number, vl_fsr_vsr_nomination_number
                                      FROM ",input_schema,".ais_vl_fsr_vsr_daily
                                      WHERE vl_fsr_vsr_voyage_number IS NOT NULL
                                      AND NOT (vl_fsr_vsr_nomination_number IS NULL or vl_fsr_vsr_nomination_number = '');
                                      
                                      
                                      -- if voyage has just one nomination, then get the nomination number
                                      
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily
                                      SET vl_fsr_vsr_nomination_number = voy_nom_comb_temp.vl_fsr_vsr_nomination_number
                                      FROM ",input_schema,".voy_nom_comb_temp
                                      WHERE eta_deviation_calc_daily.vl_fsr_vsr_nomination_number IS NULL
                                      AND eta_deviation_calc_daily.vl_fsr_vsr_voyage_number = voy_nom_comb_temp.vl_fsr_vsr_voyage_number
                                      AND eta_deviation_calc_daily.vl_fsr_vsr_voyage_number IN (SELECT vl_fsr_vsr_voyage_number
                                      FROM ",input_schema,".voy_nom_comb_temp
                                      GROUP BY vl_fsr_vsr_voyage_number
                                      HAVING COUNT(vl_fsr_vsr_nomination_number) = 1);
                                      
                                      
                                      -- if nomination has just one voyage, then get the voyage number
                                      
                                      
                                      UPDATE ",input_schema,".eta_deviation_calc_daily
                                      SET vl_fsr_vsr_voyage_number = voy_nom_comb_temp.vl_fsr_vsr_voyage_number
                                      FROM ",input_schema,".voy_nom_comb_temp
                                      WHERE eta_deviation_calc_daily.vl_fsr_vsr_voyage_number IS NULL
                                      AND eta_deviation_calc_daily.vl_fsr_vsr_nomination_number = voy_nom_comb_temp.vl_fsr_vsr_nomination_number
                                      AND eta_deviation_calc_daily.vl_fsr_vsr_nomination_number IN (SELECT vl_fsr_vsr_nomination_number
                                      FROM ",input_schema,".voy_nom_comb_temp
                                      GROUP BY vl_fsr_vsr_nomination_number
                                      HAVING COUNT(vl_fsr_vsr_voyage_number) = 1);
                                      
                                      DROP TABLE IF EXISTS ",input_schema,".voy_nom_comb_temp;
                                      
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    
    
    src_data_name <- 'eta_deviation_calc_daily'
    start_ts = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    error <- sqlQuery(channel, paste0("-- Table that drives the spotfire dashboards for eta compliance to plan view
                                      DROP TABLE IF EXISTS ",input_schema,".eta_deviation_calc_daily_spotfire;
                                      
                                      CREATE TABLE ",input_schema,".eta_deviation_calc_daily_spotfire AS
                                      WITH temp_eta_deviation AS
                                      (SELECT DISTINCT ais_destination, 
                                      ais_shiptype, 
                                      ais_vl_fsr_vsr_vessel_name, --ais_static_name, 
                                      ais_vl_fsr_vsr_commodity, 
                                      ais_vl_fsr_vsr_date_position, 
                                      ais_vl_fsr_vsr_imo, 
                                      ais_vl_fsr_vsr_incoterms, 
                                      fixed_eta_to_ph, 
                                      profiled_eta_to_ph, 
                                      profiled_deviation_hour_14days_ata, 
                                      profiled_deviation_hour_21days_ata, 
                                      profiled_deviation_hour_7days_ata, 
                                      fixed_deviation_hour_14days_ata, 
                                      fixed_deviation_hour_21days_ata, 
                                      fixed_deviation_hour_7days_ata, 
                                      s5_deviation_hour_14days_ata, 
                                      s5_deviation_hour_21days_ata, 
                                      s5_deviation_hour_7days_ata, 
                                      profiled_s5_eta_to_ph, 
                                      vl_fsr_vessel_eta, 
                                      vl_fsr_vsr_nomination_number, 
                                      vl_fsr_vsr_voyage_number, 
                                      vl_deviation_hour_14days_ata, 
                                      vl_deviation_hour_21days_ata, 
                                      vl_deviation_hour_7days_ata, 
                                      vl_voyage_operator_name,
                                      vl_vendor_name,
                                      DENSE_RANK() OVER (PARTITION BY ais_vl_fsr_vsr_imo, ais_vl_fsr_vsr_date_position
                                      ORDER BY ais_vl_fsr_vsr_imo ASC, ais_vl_fsr_vsr_date_position ASC, vl_fsr_vsr_voyage_number ASC, vl_fsr_vsr_nomination_number ASC) as rank
                                      FROM ",input_schema,".eta_deviation_calc_daily)
                                      SELECT *
                                      FROM temp_eta_deviation
                                      WHERE rank = 1
                                      AND ((vl_deviation_hour_7days_ata <= 1500 AND vl_deviation_hour_7days_ata >= -1500)
                                      or (vl_deviation_hour_14days_ata <= 1500 AND vl_deviation_hour_14days_ata >= -1500)
                                      or (vl_deviation_hour_21days_ata <= 1500 AND vl_deviation_hour_21days_ata >= -1500)
                                      or (profiled_deviation_hour_7days_ata <= 1500 AND profiled_deviation_hour_7days_ata >= -1500)
                                      or (profiled_deviation_hour_14days_ata <= 1500 AND profiled_deviation_hour_14days_ata >= -1500)
                                      or (profiled_deviation_hour_21days_ata <= 1500 AND profiled_deviation_hour_21days_ata >= -1500)
                                      or (fixed_deviation_hour_7days_ata <= 1500 AND fixed_deviation_hour_7days_ata >= -1500)
                                      or (fixed_deviation_hour_14days_ata <= 1500 AND fixed_deviation_hour_14days_ata >= -1500)
                                      or (fixed_deviation_hour_21days_ata <= 1500 AND fixed_deviation_hour_21days_ata >= -1500)
                                      or (s5_deviation_hour_7days_ata <= 1500 AND s5_deviation_hour_7days_ata >= -1500)
                                      or (s5_deviation_hour_14days_ata <= 1500 AND s5_deviation_hour_14days_ata >= -1500)
                                      or (s5_deviation_hour_21days_ata <= 1500 AND s5_deviation_hour_21days_ata >= -1500)) ;
                                      
                                      
                                      "), FALSE)
    end_ts <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S")
    if (is.integer(error)) {
      if (error == -1L){
        status = 'failed'
      }else{
        status = 'succeed'
      }
    }else{
      status = 'succeed'
    }
    sqlQuery(channel, paste0("insert into fa_d2.log_table values ('eta_deviation_calc_daily_spotfire', '",today_date,"','",start_ts,"','", status,"'," ,"'5.1. AIS_BAU_Daily_Run.R', 'AIS_BAU','", src_data_name,"','", end_ts,"')"))
    
    # Close the channel
    odbcClose(channel)
    
    break;
    
  }
  
  
  while (TRUE) {
    
    channel = odbcConnect(input_odbc_driver,uid =input_username,pwd=input_password)
    
    sqlQuery(channel, paste0("-- Drop temp tables that are no longer needed
                             DROP TABLE IF EXISTS ",input_schema,".ais_data_daily_temp;
                             DROP TABLE IF EXISTS ",input_schema,".ais_eta_standardization_daily_spotfire_temp;
                             DROP TABLE IF EXISTS ",input_schema,".ais_sap_combined_calc_temp;
                             DROP TABLE IF EXISTS ",input_schema,".ais_sap_s5_daily_temp;
                             DROP TABLE IF EXISTS ",input_schema,".ais_vl_fsr_vsr_daily_spotfire_temp;
                             DROP TABLE IF EXISTS ",input_schema,".ais_vl_fsr_vsr_daily_temp;
                             DROP TABLE IF EXISTS ",input_schema,".avg_journey_time_temp;
                             DROP TABLE IF EXISTS ",input_schema,".avg_time_at_port_future;
                             DROP TABLE IF EXISTS ",input_schema,".avg_time_spent_at_port_temp;
                             DROP TABLE IF EXISTS ",input_schema,".avg_time_spent_temp;
                             DROP TABLE IF EXISTS ",input_schema,".destination_standardization_daily_temp;
                             DROP TABLE IF EXISTS ",input_schema,".eta_deviation_calc_daily_temp;
                             DROP TABLE IF EXISTS ",input_schema,".eta_standardisation_daily_temp;
                             DROP TABLE IF EXISTS ",input_schema,".far_disport_profiler_daily_temp;
                             DROP TABLE IF EXISTS ",input_schema,".fsr_data_daily_temp;
                             DROP TABLE IF EXISTS ",input_schema,".fsr_vsr_daily_temp;
                             DROP TABLE IF EXISTS ",input_schema,".last_port_temp;
                             DROP TABLE IF EXISTS ",input_schema,".m_daily_data_combined_temp;
                             DROP TABLE IF EXISTS ",input_schema,".vl_data_daily_temp;
                             DROP TABLE IF EXISTS ",input_schema,".vl_fsr_vsr_daily_temp;
                             DROP TABLE IF EXISTS ",input_schema,".vsr_data_daily_temp;
                             DROP TABLE IF EXISTS ",input_schema,".ais_combined_daily_temp;
                             DROP TABLE IF EXISTS ",input_schema,".VL_DATA_DAILY;
                             DROP TABLE IF EXISTS ",input_schema,".FSR_Data_Daily;
                             DROP TABLE IF EXISTS ",input_schema,".VSR_Data_Daily;
                             DROP TABLE IF EXISTS ",input_schema,".FSR_VSR_Daily;
                             DROP TABLE IF EXISTS ",input_schema,".VL_FSR_VSR_Daily;
                             DROP TABLE IF EXISTS ",input_schema,".voy_nom_comb_temp;
                             
                             
                             -- VACUUM tables
                             
                             VACUUM ",input_schema,".fsr_master_daily_data;
                             VACUUM ",input_schema,".vl_master_daily_data;
                             VACUUM ",input_schema,".vsr_master_daily_data;
                             VACUUM ",input_schema,".s5_historical_dump_daily;
                             VACUUM ",input_schema,".vessel_list_imo_master;
                             VACUUM ",input_schema,".AIS_DATA_DAILY;
                             VACUUM ",input_schema,".AIS_VL_FSR_VSR_Daily;
                             VACUUM ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire;
                             VACUUM ",input_schema,".ais_sap_s5_daily;
                             VACUUM ",input_schema,".ais_sap_combined_calc;
                             VACUUM ",input_schema,".m_daily_data_combined;
                             VACUUM ",input_schema,".ais_eta_standardization_daily;
                             VACUUM ",input_schema,".ais_eta_standardization_daily_spotfire;
                             VACUUM ",input_schema,".eta_deviation_calc_daily;
                             VACUUM ",input_schema,".eta_deviation_calc_daily_spotfire;
                             
                             
                             "))
    
  
    
    for(user in input_users) {
      sqlQuery(channel, paste0("grant all privileges on table ",input_schema,".vessel_list_imo_master to ",user,";
                               grant all privileges on table ",input_schema,".AIS_DATA_DAILY to ",user,";
                               grant all privileges on table ",input_schema,".far_disport_profiler_compile to ",user,";
                               grant all privileges on table ",input_schema,".far_disport_profiler_daily to ",user,"; 
                               grant all privileges on table ",input_schema,".distance_table_daily to ",user,";
                               grant all privileges on table ",input_schema,".last_port_daily to ",user,";
                               grant all privileges on table ",input_schema,".ais_sap_s5_daily to ",user,";
                               grant all privileges on table ",input_schema,".m_daily_data_combined to ",user,";
                               grant all privileges on table ",input_schema,".fsr_master_daily_data to ",user,";
                               grant all privileges on table ",input_schema,".vl_master_daily_data to ",user,";
                               grant all privileges on table ",input_schema,".vsr_master_daily_data to ",user,";
                               grant all privileges on table ",input_schema,".s5_historical_dump_daily to ",user,";
                               grant all privileges on table ",input_schema,".AIS_VL_FSR_VSR_Daily to ",user,";
                               grant all privileges on table ",input_schema,".AIS_VL_FSR_VSR_Daily_Spotfire to ",user,";
                               grant all privileges on table ",input_schema,".ais_sap_combined_calc to ",user,";
                               grant all privileges on table ",input_schema,".eta_deviation_calc_daily to ",user,";
                               grant all privileges on table ",input_schema,".ais_eta_standardization_daily to ",user,";
                               grant all privileges on table ",input_schema,".ais_eta_standardization_daily_spotfire to ",user,";
                               grant all privileges on table ",input_schema,".eta_deviation_calc_daily_spotfire to ",user,";" ))
    }
    
    
    # Close the channel
    
    
    
    
    
    odbcClose(channel)
    
    print("*******************************************")
    print("AIS BAU Daily Run RFC Script Execution Finished")
    
    xlcFreeMemory()
    rm(list=ls())
    
    break;
    
    }
  
  
  break;
  }

print(paste0("Code running end time: ", Sys.time()))

print("*******************************************")


NULL},
     warning = function(e) {
       if (identical(e$message,"restarting interrupted promise evaluation")){
         invokeRestart("muffleWarning")
       }
     }
    ),
    expectation_failure = function(e){e},
    error = function(e){e}
  )
  
  print(paste0("tried time: ", 4-times))
  if (is.null(e)){
    return(invisible(TRUE))
  }
  times <- times - 1L
}
  
sink()     
