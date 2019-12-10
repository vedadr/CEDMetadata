####
#
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> CHECK IF ODBC CONNECTION IS CREATED FIRST!!!! <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#
#
# This script will take data from provided folder (data folder in config file).
# 1. It will use geoDivisionsBydatasetId and allGeotypesAndSumLevels to decide summary levels and split data accordingly
# 2. Add propper FIPS, NAME columns, and columns with partial fipses named as acronyms of a sumlevs (or just capitalized for sumlevs with one word)
# 3. In case data table is wider than 200 columns, it will split it into multiple tables
# 4. Write data to database
# 5. Create table_names table, later to be used in metadata file as metadata table name
# v2.06
#*****************************************************************************************************************************************


# Install all required packages if they don't exist, and load them --------


if (!require("pacman")) {
  install.packages("pacman")
  tryCatch(install.packages("pacman"), error = function(e) {
  message("Something is wrong with installation of pacman package (probably lib location), try to install it manually.")
 })
}


pacman::p_load(dplyr, RODBC, yaml, doSNOW, gtools, zoo)

library(dplyr)
library(RODBC)
library(yaml)
library(doSNOW) # for paralelization
library(gtools) # for smartbind
library(zoo)
library(pacman)
library(stringr) # for str_split_fixed

# Create clusters to run code on many cores in parallel --------------------------------------------
myCluster <- makeCluster(4, type = "SOCK")
registerDoSNOW(myCluster)

# Helper functions -------------------------------------------------------

# Perform basic checks on data --------------------------------------------

checkPrerequisites <- function(config, pathToConfig) {
  # This will check settings in config file
  #
  # @param config Full path to config file
  #

  # Check if config file exists
  if(!file.exists(pathToConfig)) {
    stop("Config file doesn't exist, check file path!!!")
  }

  if(!dir.exists(config$sourceDirectory)) {
    stop("Source directory doesn't exist!")
  }

  if(!dir.exists(config$outputDirectory)) {
    stop("Output directory doesn't exist!")
  }

  if(!dir.exists(config$configDirectory)) {
    stop("Config directory doesn't exist!")
  }

  if(!('projectId' %in% names(config))) {
    stop("Project Id not defined!")
  }

  if(config$writeToDatabase == TRUE && !('dbName' %in% names(config))) {
    stop("Database name is not defined!")
  }

  if(config$writeToDatabase == TRUE && !('server' %in% names(config))) {
    stop("Server name is not defined!")
  }

  if(config$writeToDatabase == TRUE && !('user' %in% names(config))) {
    stop("Database Username is not defined!")
  }

  if(config$writeToDatabase == TRUE && !('password' %in% names(config))) {
    stop("Database Password is not defined!")
  }

  if(!('projectYear' %in% names(config))) {
    stop("Project year is not defined!")
  }
}

writeTableNames <- function(outputDirectory = NA, exportedFilesList, dbName, serverName, user, password,trustedConn) {
  # This will create table table_names in database that contains connection between table suffixes and source file names,
  # to be used as table name when metadata is generated
  #
  # @param outputDirectory Needed only in case csv is beeing written, default NA
  # @param exportedFilesList List of file names from which the data was taken
  # @param dbName Database name
  # @param serverName Server name
  # @param user SQL server User name
  # @param password SQL Server password
  # @param trustedConn flag if connection is trusted or not (use windows login or not),usualy for debuging on local it is set to TRUE
  # @return Write table_names table to database and possibly to csv

  print("Writing table_names!")
  if(trustedConn) {
    conn <- odbcDriverConnect(paste('driver={SQL Server};server=',serverName,';database=',dbName,';trusted_connection=yes', sep = ''))
  } else {
    conn <- odbcConnect(serverName, uid=user, pwd = password)
  }
  writeFile <- paste(outputDirectory, 'table_names.csv',sep='')
  tryCatch({
    sqlSave(conn, as.data.frame(exportedFilesList), tablename = 'table_names', rownames = F, safer = TRUE, append = TRUE)
    odbcCloseAll()
  }, error = function(e) {
    print("There's been an error, maybe table already exists? Trying to delete it and create it again.")
    print(e)
    print("Dropping!")
    sqlDrop(conn, 'table_names')
    sqlSave(conn, as.data.frame(exportedFilesList), tablename = 'table_names', rownames = FALSE, safer = TRUE, append = TRUE)
    odbcCloseAll()
  })

  print("Done")
}

createAcronym <- function(texts) {
  # This is used to create partial fips names from geography names
  #
  # @param texts vector of words or single word to be acronymized 8)
  # @return Vector of accronyms in case of multiple words, or just one acronym if one word is provided

  for(i in 1:length(texts)) {
    acronyms <- as.character()
    # replace weird characters with blank space
    charsForRemoval <- c('(',')','&','/',',','.','\\','\'','&','%','#')
    texts[i] <- paste(unlist(sapply(unlist(strsplit(texts[i],'')), function(x) {
      if(x %in% charsForRemoval) {
        ' '
      } else {
        x
      }
    })), collapse = '')

    if(!grepl(' ',texts[i])) {

      acronyms <- toupper(texts[i])
    } else {
      # use the same logic as python script
      # remove all double spaces if any
      if('  ' %in% unlist(strsplit(texts[i],''))) {
        while('  ' %in% unlist(strsplit(texts[i],''))) {
          texts[i] <- gsub('  ', ' ', texts[i])
        }
      }

      # take only first letters of the words, capitalize it and then convert it into string
      acronyms <- paste(sapply(unlist(strsplit(texts[i], ' ')), function(x) {
        toupper(substr(x, 1, 1))
      }), collapse ='')
    }
  }
  acronyms
}

setPrimaryKeys <- function(tableSumlev, exportedFileName, geoLevelInformation, conn, geoIdColumnType) {
  # Because sqlSave method doesn't create propper primary keys this function is needed to fix it after the tables are created
  #
  # @param tablesumlev Summary level for table
  # @param exportedFileName Final name for the table in database
  # @param geoLevelInformation Dataframe with geo level information
  # @param conn Connection handle
  # @param geoIdColumnType if numeric set to INT else set it to NVARCHAR(10)
  # @return Create primary keys on already created tables in database

  primaryKeys <- as.character()
  pos <- grep(substr(tableSumlev,1,5), geoLevelInformation$Geo_level)
  previousIndent <- 999 # set dummy number for first pass
  for(i in rev(1:pos)) {
    if(previousIndent == geoLevelInformation[i,'Indent']) { # skip Geo_levels with same indent
      next
    }

    primaryKeys <- c(primaryKeys, paste(geoLevelInformation[i, 'Geo_level'], '_FIPS',sep=''))

    # when last indent reached break
    if( geoLevelInformation[i,'Indent'] == 0) {
      break
    } else {
      primaryKeys <- c(primaryKeys,', ')
    }
    previousIndent <- geoLevelInformation[i,'Indent']
  }

  geoIdColumnType <- if(geoIdColumnType == 'numeric') {
    'INT'
  } else {
    'NVARCHAR(10)'
  }

  # execute queries

  for(i in primaryKeys) {
    query = paste('ALTER TABLE ', exportedFileName, ' ALTER COLUMN ', i,' ', geoIdColumnType, ' NOT NULL',sep='')
    sqlQuery(conn, query)
  }
  query = paste('ALTER TABLE ', exportedFileName, ' ADD PRIMARY KEY (', paste(primaryKeys, sep = '', collapse = ''), ')' ,sep='')
  sqlQuery(conn, query)
}


prepareTableNames <- function(projectYear, serverName, user, password, dbName, trustedConn) {
  # This will prepare table_names table, it will first try to drop everything from that project Year, if failed it will try to drop it and create empty table
  #
  # @param projectYear Project year
  # @param serverName DB connection information
  # @param user DB connection information
  # @param password DB connection information
  # @param dbName DB connection information
  # @return  Prepare table_names table in database

  if(trustedConn) {
    conn <- odbcDriverConnect(paste('driver={SQL Server};server=',serverName,';database=',dbName,';trusted_connection=yes', sep = ''))
  } else {
    conn <- odbcConnect(serverName, uid=user, pwd = password)
  }
  query <- paste("DELETE FROM table_names WHERE projectYear = '", projectYear, "'", sep='')
  tryCatch({
    sqlQuery(conn, query)
    odbcCloseAll()}, error = function(e) {
      try(sqlDrop(conn, 'table_names', errors = FALSE), silent = TRUE)
      sqlSave(conn, data.frame(descName = character(), codeName = character(), projectYear = character()), tablename = 'table_names', rownames = FALSE)
      odbcCloseAll()
    })
}

# createNameAndFipsColumns <- function(dataTable, geoLevelInformation) {
#   addDf <- as.data.frame(matrix(data = NA, nrow = nrow(dataTable[1]), ncol = length(geoLevelInformation$Geo_level)))
#   names(addDf) <- sapply(geoLevelInformation$Geo_level, function(x) paste0(x,'_FIPS'))
#   dataTable <- cbind(dataTable, addDf)
#
#   addDfName <- as.data.frame(matrix(data = NA, nrow = nrow(dataTable[1]), ncol = length(geoLevelInformation$Geo_level)))
#   names(addDfName) <- sapply(geoLevelInformation$Geo_level, function(x) paste0(x,'_NAME'))
#   dataTable <- cbind(dataTable, addDfName)
#
#   for(i in 1:nrow(dataTable)) {
#     dataTable <- fillRowFips(i, dataTable, geoLevelInformation)
#
#   }
#
#
#   for(i in 1:nrow(dataTable)) {
#     dataTable <- fillRowName(i, dataTable, geoLevelInformation)
#   }
#
#   dataTable
# }
#
# fillRowFips <- function(rowNumber, changeDf, geoLevelInformation) {
#   tmpLen <- 0
#   tmpInd <- 0
#
#   slPos <- which(geoLevelInformation$Geo_level == changeDf$SUMLEV[rowNumber])
#   for(i in slPos:1){
#     if(slPos == i) {
#       changeDf[rowNumber, paste0(geoLevelInformation$Geo_level[i],'_FIPS')] <-substr(as.character(changeDf$Geo[rowNumber]), nchar(as.character(changeDf$Geo[rowNumber]))-as.numeric(changeDf$PARTIAL_FIPS_length[rowNumber])+1, nchar(as.character(changeDf$Geo[rowNumber])))
#       tmpInd <- geoLevelInformation$Indent[i]
#       tmpLen <- as.numeric(changeDf$PARTIAL_FIPS_length[rowNumber])
#     } else if(geoLevelInformation$Indent[i] >= tmpInd) {
#       next
#     } else {
#       if(i == 1) {
#         changeDf[rowNumber, geoLevelInformation$Geo_level[i]] <- "0"
#       } else {
#         tmpLen <- tmpLen + as.numeric(geoLevelInformation$PARTIAL_FIPS_length[i])
#         tmpParFips <- substr(substr(as.character(changeDf$Geo[rowNumber]), nchar(as.character(changeDf$Geo[rowNumber]))-tmpLen+1, nchar(as.character(changeDf$Geo[rowNumber]))), 0,geoLevelInformation$PARTIAL_FIPS_length[which(geoLevelInformation$Geo_level == geoLevelInformation$Geo_level[i])])
#         changeDf[rowNumber, paste0(geoLevelInformation$Geo_level[i],'_FIPS')] <- tmpParFips
#         tmpInd <- geoLevelInformation$Indent[i]
#       }
#     }
#   }
#   return(changeDf)
# }

createNameAndFipsColumns <- function(dataTable, sumLevelsForDataset, allGeotypesAndSumLevels) {

  for(n in sort(sumLevelsForDataset,decreasing = T)) {
    summaryIdentifiers <- filter(allGeotypesAndSumLevels, SUMLEV == n)
    colnames(summaryIdentifiers)[colnames(summaryIdentifiers)=='NAME'] <- 'NAME_geo'
    dataTable <- merge(dataTable, summaryIdentifiers[,c(1:2,1)], by.x = 'Geo', by.y = 'FIPS', all.x = TRUE)

    # create partial fips from the whole unless full fips is 0
    dataTable$FIPS.1 <- ifelse(dataTable$PARTIAL_FIPS_length != 0,ifelse(!is.na(dataTable$FIPS.1),substr(dataTable$FIPS.1,nchar(dataTable$FIPS.1)-as.numeric(dataTable$PARTIAL_FIPS_length)+1, length(dataTable$FIPS.1)),NA),dataTable$FIPS.1)

    colnames(dataTable)[colnames(dataTable)=='NAME_geo'] <- paste(n,'_NAME',sep='')
    colnames(dataTable)[colnames(dataTable)=='FIPS.1'] <- paste(n,'_FIPS',sep='')
  }

  dataTable <- dataTable[order(dataTable$Geo),]
  dataTable[,grep('_FIPS$', names(dataTable))] <- dataTable[,sort(grep('_FIPS$', names(dataTable)), decreasing = T)]
  dataTable[,grep('_NAME$', names(dataTable))] <- dataTable[,sort(grep('_NAME$', names(dataTable)), decreasing = T)]

  print("Creating nesting for FIPS!")

  # in case there is just one geo level then no nesting is needed
  if(length(grep("_FIPS$", names(dataTable))) == 1) {
   message("Info: Only one geo in this dataset!? No nesting is being done!")
   return(dataTable)
  }

  dataTable[,sort(grep("_FIPS$", names(dataTable)), decreasing = T)] <- t(apply(dataTable[,grep("_FIPS$", names(dataTable))], 1, function(x) {
    notNaPos <- unname(which(!is.na(x)))
    cnt<-1
    rowLine <- character()
    for (y in x) {
      if(cnt < notNaPos) {
        rowLine <- c(rowLine,NA)
      }else if (cnt == notNaPos){
        rowLine <- c(rowLine,y)
      } else {
        rowLine <- c(rowLine,'--REMOVE--')
      }
      cnt <- cnt +1
    }
    rowLine
  }))
  print("Done")

  print("Creating nesting for NAME!")
  dataTable[,sort(grep("_NAME$", names(dataTable)), decreasing = T)] <- t(apply(dataTable[,grep("_NAME$", names(dataTable))], 1, function(x) {
    notNaPos <- unname(which(!is.na(x)))
    cnt<-1
    rowLine <- character()
    for (y in x) {
      if(cnt < notNaPos) {
        rowLine <- c(rowLine,NA)
      }else if (cnt == notNaPos){
        rowLine <- c(rowLine,y)
      } else {
        rowLine <- c(rowLine,'--REMOVE--')
      }
      cnt <- cnt +1
    }
    rowLine
  }))

  dataTable[,grep("_FIPS$", names(dataTable))] <- na.locf(dataTable[,grep("_FIPS$", names(dataTable))])
  dataTable[,grep("_NAME$", names(dataTable))] <- na.locf(dataTable[,grep("_NAME$", names(dataTable))])
  dataTable[,grep("_FIPS$|_NAME$", names(dataTable))][dataTable[,grep("_FIPS$|_NAME$", names(dataTable))] == '--REMOVE--']  <- NA
  print("Done")

  dataTable
}

fixLeadingZeros <- function(dataTable, geoLevelInformation) {
  dataTable$Geo <- apply(dataTable, 1, function(x) {
    incompleteGeo <- x['Geo']
    propperLength <- geoLevelInformation[geoLevelInformation$Geo_level == x['SUMLEV'], 'FIPS_length']

    if(propperLength == 0 && nchar(incompleteGeo) == 1) {
      incompleteGeo <- paste0('0', incompleteGeo)
    } else if (propperLength != nchar(incompleteGeo)) {
      while(nchar(incompleteGeo) < propperLength && propperLength != 0) {
        incompleteGeo <- paste0('0', incompleteGeo)
      }
    }
    incompleteGeo
  })

  dataTable
}

#******************************************************************************************************
#
# These functions will create missing tables on other levels  -------------
#
#******************************************************************************************************


createEmptyDfForLevel <- function(missingTableName, conn, tableNames, firstFullTable) {
  resultDf <- data.frame()

  #create SLXXX part of empty dataframe
  assign(unlist(strsplit(missingTableName,"_"))[2], select(sqlFetch(conn, grep(unlist(strsplit(missingTableName,"_"))[2],firstFullTable, value = TRUE)), -starts_with("CANCEN2011")))

  # 1. odrezati sufix
  sufix <-substr(missingTableName,unlist(gregexpr('_',missingTableName))[length(unlist(gregexpr('_',missingTableName)))]+1,nchar(missingTableName))
  # 2. u postojecim naci jednu koja posotji da bi uzeli imena varijabli
  dataColumnNames <- names(select(sqlFetch(conn, grep(paste(sufix,'$',sep=''), tableNames, value= TRUE)[1]), starts_with("NHS2015")))
  # 2a. Find variable datatypes
  dataTypes <- sapply(select(sqlFetch(conn, grep(paste(sufix,'$',sep=''), tableNames, value= TRUE)[1]), starts_with("NHS2015")), class)

  # 3. napraviti prazan df koji ima imena kolona kao imena varijabli
  emptyDf <- as.data.frame(matrix(data=NA, nrow = nrow(get(unlist(strsplit(missingTableName,"_"))[2])), ncol = length(dataColumnNames)))
  # 3. Set propper column types
    for (i in 1:length(emptyDf)) {
      emptyDf[,i] <-  switch(dataTypes[i],
                            "character" = as.character(emptyDf[,i]),
                            "integer" = as.numeric(emptyDf[,i]),
                            "double" = as.double(emptyDf[,i]),
                            "numeric" = as.double(emptyDf[,i]),
                            "factor" = as.character(emptyDf[,i])
                            )
    }


  #
  # 4. spojiti sa odgovarajucim SLXXX df
  names(emptyDf) <- dataColumnNames
  resultDf <- cbind(get(unlist(strsplit(missingTableName,"_"))[2]), emptyDf)
  # 5. return

  # TODO check why was this needed and uncomment it?
  # resultDf[, grep("FIPS",names(resultDf))] <- as.data.frame(apply(resultDf[, grep("FIPS",names(resultDf))], 2, as.character), stringsAsFactors = F)
  resultDf
}


createMissingTables <- function(config) {
  print("Creating missing tables in database!")

  # 1. Load all sumlevs from cfg file
  #config <- yaml.load_file("C:/Projects/ContentProduction/Canadian Census/scripts/postprocessing - R/config.yml")
  geoLevelInformation <- as.data.frame(t(as.data.frame(config$geoLevelInfo)),stringsAsFactors = F)
  names(geoLevelInformation) <- c("Geo_level","Geo_level_name","FIPS_length","PARTIAL_FIPS_length","Indent")

  # 2. Find current tables in sql and create vector with suffixes
  # connection string

  if(config$writeToDatabase == TRUE) {
    if (config$trustedConnection) {
      conn <- odbcDriverConnect(paste('driver={SQL Server};server=',config$server,';database=',config$dbname,';trusted_connection=yes', sep = ''))
    } else {
      conn <- odbcConnect(config$server, uid=config$user, pwd = config$password)
    }
  }

  sql <- "select name
  from sys.objects
  where type_desc = 'USER_TABLE'
  and name != 'table_names'"

  tableNames <- sqlQuery(conn, sql, stringsAsFactors = F)
  tableNamesSplited <- as.data.frame(str_split_fixed(tableNames$name, "_", 3))
  sufixUnique <-as.data.frame(unique(tableNamesSplited$V3))
  tableNames <- as.vector(t(tableNames))
  names(sufixUnique)[names(sufixUnique)=="unique(tableNamesSplited$V3)"] <- "sufix"


  # 3. Find missing and create new tables
  # Find first full table for each of the sumlevs, used to take system columns from
  tableNames <- tableNames[grep('^_',tableNames, invert = TRUE)] # exclude table with leading '_'
  dupli <- !(duplicated(substr(sort(tableNames),12,16)))
  # crfeate list of first full tables before we create _001 tables for all geos
  firstFullTable <- sort(tableNames)[dupli]

  MissingTables <- character()
  for (i in geoLevelInformation$Geo_level) {

    for(j in sufixUnique$sufix) {

      if(!(paste(tableNamesSplited[1,1],"_",i,"_",j,sep="") %in% tableNames)){

        MissingTables <- c(MissingTables,paste(tableNamesSplited[1,1],"_",i,"_",j,sep=""))
      }
    }
  }

  print("creating tables")

  dataSetIds <- unique(unlist(sapply(tableNames, function(x){ # get list of available datasets in db
    unlist(strsplit(tableNames,'_'))[1]
  })))

  ## First create 001 tables for each summary level
  # find columns for geographies that doesn't yet exist in the data
  mandatoryColumns <- sapply(dataSetIds, function(x) grep(x,names(sqlFetch(conn, tableNames[1])), value = T, invert = T)) # exclude all columns with dataset ids in column name
  fakeDataframe <- data.frame(matrix(data = '', nrow = 1, ncol = length(mandatoryColumns)))
  names(fakeDataframe) <- mandatoryColumns


  sapply(unique(geoLevelInformation$Geo_level), function(x) { # added unique to get rid of the multiple 270's
    sapply(dataSetIds, function(ds) {
      tryCatch({
        if(any(grepl(x,firstFullTable))) {
          sqlSave(conn, createEmptyDfForLevel(paste(ds,'_',x,'_005', sep = ''), conn, tableNames, firstFullTable), tablename = paste(ds,'_',x,'_005', sep = ''), rownames = F)
          MissingTables <- MissingTables[MissingTables != paste(ds,'_',x,'_001', sep = '')]
        } else {
          sqlSave(conn, fakeDataframe, tablename = paste(ds,'_',x,'_005', sep = ''))
          MissingTables <- MissingTables[MissingTables != paste(ds,'_',x,'_005', sep = '')]
        }
        #}
      }, error = function(e){
        #print(e)
        message(paste("Table ",ds,'_',x,'_005 cannot be written:',e, sep = ''))
      })
    })
  })

  ## Create missing tables for existing geographies
  ############### from here
  # for (i in MissingTables){
  #   if(!(any(grepl(unlist(strsplit(i,"_"))[6],firstFullTable)))) {
  #     next
  #   }
  #   tryCatch({
  #     sqlSave(conn,createEmptyDfForLevel(i, conn, tableNames, firstFullTable),tablename = i,rownames = F)
  #     setPrimaryKeys(unlist(strsplit(i,"_"))[6], i, geoLevelInformation, conn, "character")
  #   }, error = function(e) {
  #     print(paste("There's been and error while writing table: ",i, ", probably already exists!?" ))
  #     print(e)
  #     #        sqlDrop(conn, i)
  #     #       sqlSave(conn,createEmptyDfForLevel(i, conn, tableNames, firstFullTable),tablename = i,rownames = F)
  #     #       setPrimaryKeys(unlist(strsplit(i,"_"))[6], i, geoLevelInformation, conn, "character")
  #   })
  # }

  ############## up to here
  print("All done!")
  odbcCloseAll()
}

# fillRowName <- function(rowNumber, changeDf, geoLevelInformation) {
#   slName <- as.character()
#   tmpInd <- 0
#   slPos <- which(geoLevelInformation$Geo_level == changeDf$SUMLEV[rowNumber])
#   for(i in slPos:1){
#     if(slPos == i) {
#       tmpInd <- geoLevelInformation$Indent[i]
#       slName <- changeDf$NAME[rowNumber]
#     } else if(geoLevelInformation$Indent[i] >= tmpInd) {
#       next
#     } else {
#       slName <- changeDf[rowNumber, which(paste0(geoLevelInformation$Geo_level[i],'_NAME')==names(changeDf))] # this was commented
#     }
#     changeDf[rowNumber, paste0(geoLevelInformation$Geo_level[i],'_NAME')] <- slName
#   }
#   return(changeDf)
# }

#******************************************************************************************************
#
# This is the main function for data processing  -------------
#
#******************************************************************************************************
processFile <- function(datasetId, fileName, counter, writeToDatabase, sourceDirectory, outputDirectory,  configDirectory, projectId, dbName, serverName, geoLevelInfo, projectYear, user, password, trustedConn) {
  #
  # Create names and fips columns, name columns, qname, divide tables if over allowed size, split them by sumlev, write it to a database and return dataframe with list of tables
  #
  # @param datasetId Dataset id being processed i.e. first part of the data file name e.g. 'CANCEN11', 'FBI10' etc.
  # @param fileName File name where data is stored
  # @param counter External counter used for table ordering
  # @param writeToDatabase Flag if data should be written to database, used for debuging, 'dry runs' etc.
  # @param sourceDirectory Directory where data files are stored
  # @param outputDirectory Directory where metadata file will be placed (or csvs if enabled)
  # @param configDirectory Directory which contains geo_divisions_by_dataset_ID.txt and all_geotypes_and_sumlev.csv files
  # @param projectId Project id
  # @param dbName Database name
  # @param serverName Server name
  # @param geoLevelInfo Geo level informations with nesting, fips length and indents
  # @param projectYear Project year
  # @param user Username on SQL server
  # @param password Password on SQL server
  # @return Process tables and return processed file name with table suffix and project year

  if(writeToDatabase == TRUE) {
    if (trustedConn) {
      conn <- odbcDriverConnect(paste('driver={SQL Server};server=',serverName,';database=',dbName,';trusted_connection=yes', sep = ''))
    } else {
      conn <- odbcConnect(serverName, uid=user, pwd = password)
    }
  }
  print('Importing files!')

  #importing the csv files we need
  geoDivisionsBydatasetId <- read.csv(paste(configDirectory,'geo_divisions_by_dataset_ID.txt',sep=''), header = FALSE, stringsAsFactors = FALSE) #_GROUPED

  geoDivisionsBydatasetId <- filter(geoDivisionsBydatasetId,V1 == datasetId)

  #check if geodivisions have levels listed in it
  if(nrow(geoDivisionsBydatasetId) == 0) {
    stop("It appears that no summary levels exist in this dataset!? Check if source data (csv's) names are prefixed with dataset id according to info in geo_divisions_by_dataset_ID.txt file!")
  }

  dataTable <- read.csv(paste(sourceDirectory, fileName,sep=''), stringsAsFactors = FALSE, colClasses = c("Geo" = "character"), check.names = F)
  originalObsNumber <- length(dataTable$Geo) # used later for error checking

  if('FIPS' %in% names(dataTable)) {
    dataTable <- select(dataTable, -contains('FIPS'))
    #dataTable <- fixFips(dataTable) only required for canadian
  }
  allGeotypesAndSumLevels <- read.csv(paste(configDirectory,'all_geotypes_and_sumlev.csv',sep='') , colClasses = c('FIPS' = 'character'), stringsAsFactors = F, encoding = 'UTF-8') # summary information TODO rename file with lowercase letter and use allGeoTypesAndSumlev

  # get type and name only for required sumlevels
  geoTypeAndSumLevels <- merge(allGeotypesAndSumLevels, geoDivisionsBydatasetId, by.x = 'TYPE', by.y = 'V2')

  # take geo informations from config file
  geoLevelInformation <- as.data.frame(t(as.data.frame(geoLevelInfo)),stringsAsFactors = F)
  names(geoLevelInformation) <- c("Geo_level","Geo_level_name","FIPS_length","PARTIAL_FIPS_length","Indent")

  print("Done!")

  print("Process data!")

  #******************************************************************************************************
  # MOST IMPORTANT MERGING IS HERE !!! IF ERROR OCCURS CHECK HERE FIRST
  # merge two df in order to get geoName
  # In case thate SUMLEV column exists in data use it to get additional geo information, else use FIPS
  # First approach is used if there are many geographies with same FIPS length making it impossible to join tables only by FIPS

  # fix formissing leading zeros in the Geo column
  # dataTable <- fixLeadingZeros(dataTable, geoLevelInformation)

  if('SUMLEV' %in% colnames(dataTable)) {
    dataTable <- merge(dataTable, geoTypeAndSumLevels, by.x = c('SUMLEV','Geo'), by.y = c('SUMLEV','FIPS'))
  } else {
    dataTable <- merge(dataTable, geoTypeAndSumLevels, by.x = 'Geo', by.y = 'FIPS')
  }

  dataTable <- merge(dataTable, geoLevelInformation, by.x = 'TYPE', by.y = 'Geo_level_name')

  if(length(dataTable$Geo) < originalObsNumber) {
    message("Warning: Number of observations has decreased, some data may be lost in process!")
  }


  # check if any important object has 0 observations, this is important for debuging purposes
  for (i in c('dataTable','geoLevelInformation','geoTypeAndSumLevels','allGeotypesAndSumLevels','geoDivisionsBydatasetId')) {
      if(length(get(i)) == 0 || nrow(get(i)) == 0) {
        message(paste0("Warning: Object ",i," has 0 observations!"))
      }
  }

    # get levels for dataset e.g. SL100, SL200 etc.
  sumLevelsForDataset <- unique(merge(allGeotypesAndSumLevels, geoDivisionsBydatasetId,by.x="TYPE", by.y = "V2")$SUMLEV)
  #sumLevelsForDataset <- unique(dataTable$Geo_level)

  #merge to get NA-s
  allOtherGeo <- allGeotypesAndSumLevels[allGeotypesAndSumLevels$SUMLEV %in% as.vector(sumLevelsForDataset),]
  missingGeos <- setdiff(allOtherGeo$FIPS, dataTable$Geo)

  if(length(missingGeos) != 0) { # if there are missing geos then run this
    allOtherGeo$Geo <- allOtherGeo$FIPS # TODO remove this after Geo id column name is changed in all files
    allOtherGeo <- merge(allOtherGeo,as.data.frame(missingGeos), by.x = 'Geo', by.y = 'missingGeos')
    # names(allOtherGeo)[names(allOtherGeo) == 'SUMLEV'] <- 'Geo_level'
    allOtherGeo$Geo_level <- allOtherGeo$SUMLEV
    allOtherGeo <- merge(allOtherGeo, geoLevelInformation[,c('Geo_level','Indent', 'FIPS_length', 'PARTIAL_FIPS_length')],by.x = 'SUMLEV', by.y = 'Geo_level')
    dataTable <- smartbind(dataTable, select(allOtherGeo,-FIPS)) # exclude FIPS column from allOtherGeo
  }

  # if(counter == 205) {
  #   print('stop')
  # }

  # create NAME and FIPS columns
  dataTable <- createNameAndFipsColumns(dataTable, sumLevelsForDataset, allGeotypesAndSumLevels)
  # dataTable <- createNameAndFipsColumns(dataTable, geoLevelInformation)

  dataTable$FIPS <- dataTable$Geo

  # new method of QNAME creation
  qnameColumn <- as.character()
  geoLevelPos <- grep('Geo_level',names(dataTable))
  for(x in  1:nrow(dataTable)){
    if(is.na(dataTable[x, geoLevelPos])) {
      qnameColumn <- c(qnameColumn,NA)
      next
    }
    qname <- character()
    previousIndent <- 9999 # dummy value for first pass
    for(rowpos in rev(1:grep(dataTable[x, geoLevelPos], geoLevelInformation$Geo_level))) {
      if(previousIndent == geoLevelInformation[rowpos,'Indent']) {
        next
      }
      qname <- c(qname,dataTable[x,paste(geoLevelInformation[rowpos, 'Geo_level'],'_NAME',sep='')]) # take indent + 1 element from geoLevelInformation
      if(geoLevelInformation[rowpos,'Indent'] == 0) {
        break
      }
      previousIndent <- geoLevelInformation[rowpos,'Indent']
      qname <- c(qname,', ')
    }
    qnameColumn <- c(qnameColumn,paste(qname,sep='', collapse='')) # concatenate strings from qname vector into one string and add it to the column vector
  }
  dataTable$QName <- qnameColumn

  dataTable$Name <- dataTable$NAME.x
  print("Done!")

  print("Renaming FIPS columns!")
  for (i in sumLevelsForDataset){
    colnames(dataTable)[colnames(dataTable)==paste(as.character(i),'_FIPS.1',sep='')] <- paste(as.character(i),'_FIPS',sep='')
  }
  print("Done!")



  #******************************************************************************************************
  ## FIX FOR BAD QNAMES CAUSED BY ARTIFICIAL NESTING LEVELS (introduced for propper summarization)

  dataTable$QName <- gsub(', NA', '', dataTable$QName)

  #******************************************************************************************************

  #separate into individual tables
  #first put geo and type on the end of the dataframe
  dataTable <- dataTable[,c(3:length(dataTable),1:2)]

  print("Spliting files by sumlev")
  for (i in sumLevelsForDataset) {
    assign(i,filter(dataTable, Geo_level == i )) # TODO bio sumlev
  }

  for (i in sumLevelsForDataset){
    shortSumLevelsForDataset <- sumLevelsForDataset[sumLevelsForDataset > i]
    if (length(shortSumLevelsForDataset) > 0) {
      for (j in shortSumLevelsForDataset) {
        nameColumn = paste(j,'_NAME',sep='')
        fipsColumn = paste(j,'_FIPS',sep='')
        df <- get(i)
        df <- select(df, -(nameColumn), -(fipsColumn))
        df[,c(nameColumn, fipsColumn )] <- NA
        assign(i, df)
      }
    }
  }

  # Create partial fips columns named as geography levels (e.g. STATE, COUNTY, CD etc.)
  tempVarNames <- as.character()
  for (sumlvl in sumLevelsForDataset) {
    tempVarNames <- as.character()
    df <- select(get(sumlvl), ends_with('_FIPS', ignore.case = FALSE))
    for( i in colnames(df)) {
      tempVarNames <- c(tempVarNames,toupper(createAcronym(filter(geoLevelInformation, Geo_level == gsub('_FIPS','',i))[,2])))
    }
    colnames(df) <- tempVarNames

    # add empty SLXXX_FIPS, SLXXX_NAMES and geo names columns for all geographies not present in curent dataset
    colsToBeCreated <- character()
    colsToBeCreated <- unique(as.vector(apply(geoLevelInformation, 1, function(x) {
      c(colsToBeCreated, paste(x['Geo_level'], '_FIPS', sep = ''), paste(x['Geo_level'], '_NAME', sep = ''), createAcronym(x['Geo_level_name']))
    })))
    colsToBeCreated <- unlist(sapply(colsToBeCreated, function(x) {
      if (!(x %in% names( cbind(get(sumlvl), df)))) {
        x
      }
    }))
    df[, colsToBeCreated] <- NA

    assign(sumlvl, cbind(get(sumlvl), df))
    # Add newly created column names into tempVarNames vector to avoid name change when variable names prefixes are being added later on
    columnsNotToAddPrefix <- c(tempVarNames, colsToBeCreated)
  }

  # set maximum number of data columns in table (FIPS, NAMES and other system required columns not included)
  tableWidth <- 200

  #if tables contains more than specified number of columns, divide them before writing
  if(length(grep(datasetId,names(dataTable), value = T))/tableWidth > 1) {
    numberOfTables = ceiling(length(grep(datasetId,names(dataTable), value = T))/tableWidth)
    # later used to append new sumlev names (names with ordered numbers)
    tempSumLevelsForDataset <- as.character()
    for(sumlvl in sumLevelsForDataset) {
      systemColumns <- grep(datasetId,names(get(sumlvl)), value = T, invert = T) # get system required columns for every table, FIPS, QNAME etc.
      for(rnr in 1:numberOfTables) {
        if(rnr == numberOfTables) {
          if(nchar(as.character(rnr))> 3) {
            tab_suff = substr(as.character(10000+rnr),2,5)
          } else {
            tab_suff = substr(as.character(1000+rnr),2,4)
          }
          assign(paste(sumlvl, '_', tab_suff,sep = ''), cbind(get(sumlvl)[,systemColumns],get(sumlvl)[,(((rnr-1)*tableWidth)+1):(length(colnames(dataTable)[!(colnames(dataTable) %in% systemColumns)]))]))
        } else {
          assign(paste(sumlvl, '_', tab_suff,sep = ''), cbind(get(sumlvl)[,systemColumns],get(sumlvl)[,(((rnr-1)*tableWidth)+1):(rnr*tableWidth)]))
        }
      }
      # crate new sumLevelsForDataset vector with info about separated files
      for(tn in 1:numberOfTables) {
        if(nchar(as.character(tn))> 3) {
          tab_suff = substr(as.character(10000+tn),2,5)
        } else {
          tab_suff = substr(as.character(1000+tn),2,4)
        }
        tempSumLevelsForDataset <- c(tempSumLevelsForDataset, paste(sumlvl, '_', tab_suff, sep = ''))
      }
    }
    sumLevelsForDataset <- tempSumLevelsForDataset
  }


    # #Uncomment this to write into csv format
    # print('Eksporting to CSV!')
    # for (n in sumLevelsForDataset){
    #   dataForWrite <- get(n)
    #
    #   dataForWrite<- select(dataForWrite, -SUMLEV,-Geo,-TYPE, -Geo_level, -V1) # remove not needed columns
    #
    #   # add preffix to all variable names
    #   names(dataForWrite) <- sapply(names(dataForWrite), function(x){
    #     if(any(grepl('FIPS',x)) || any(grepl('NAME',x,ignore.case = T) || any(grepl('Geo',x))) || x %in% columnsNotToAddPrefix) {
    #       x } else {
    #         paste(projectId,'_',substr(as.character(counter+1000),2,4),'_',x,sep='') #,'_',n
    #       }
    #   })
    #   # name of the final file/table
    #   exportedFileName = paste(projectId,'_',n,'_',substr(as.character(counter+1000),2,4),sep = '')
    #
    #   write.csv(dataForWrite,paste(outputDirectory,exportedFileName,'.csv',sep=''), row.names = F)
    # }
    # print("Done!")
    #
  exportedFilesList <- data.frame(descName = character(), codeName = character()) # this will hold a list of processed tables

  if (writeToDatabase == TRUE) {
    print("Eksporting to DB!")
    message("Writing to database!")
    for (m in sumLevelsForDataset){
      dataForWrite <- get(m)

      # remove duplicated columns if any
      if (any(duplicated(colnames(dataForWrite)))) {
        message("Warning: Duplicated columns detected and will be removed!!!")
        dataForWrite <- dataForWrite[,!duplicated(colnames(dataForWrite))]
      }
      dataForWrite<- select(dataForWrite, -SUMLEV,-Geo,-TYPE) # remove unneeded columns

      # add preffix to all variable names
      names(dataForWrite) <- sapply(names(dataForWrite), function(x){
        if(any(grepl('FIPS',x)) || any(grepl('NAME',x,ignore.case = T) || any(grepl('Geo',x))) || x %in% columnsNotToAddPrefix) {
          x } else {
            if(nchar(as.character(counter))> 3) {
              tab_suff = substr(as.character(10000+counter),2,5)
            } else {
              tab_suff = substr(as.character(1000+counter),2,4)
            }
            paste(projectId,'_', tab_suff,'_',x,sep='') #'_',m,
          }
      })

      # Data eksport ------------------------------------------------------------
      if(nchar(as.character(counter))> 3) {
        tab_suff = substr(as.character(10000+counter),2,5)
      } else {
        tab_suff = substr(as.character(1000+counter),2,4)
      }
      if (length(grep(datasetId,names(dataTable), value = T))/tableWidth > 1) {
        exportedFileName <- paste(projectId,'_',m,tab_suff,sep = '')
        exportedFilesList <- rbind(exportedFilesList, data.frame(descName = paste(fileName, '_', substr(m,7,13), sep=''),codeName = paste(substr(m,7,13), tab_suff,sep ='')))
      } else {
        exportedFileName <- paste(projectId,'_',m,'_',tab_suff,sep = '')
        exportedFilesList <- rbind(exportedFilesList, data.frame(descName = fileName,codeName = tab_suff))
      }

      tryCatch({
        print(exportedFileName)
        # if(grepl('SL010', exportedFileName)) {
        sqlSave(conn,dataForWrite, tablename = exportedFileName, rownames = F)
        setPrimaryKeys(m, exportedFileName, geoLevelInformation, conn, typeof(dataTable$Geo))
        #
        # } else {
        #   print(paste0('other level , skipping', exportedFilesList))
        # }
      }, error = function(e) {
        sqlDrop(conn, exportedFileName)
        sqlSave(conn,dataForWrite, tablename = exportedFileName, rownames = F)
        setPrimaryKeys(m, exportedFileName, geoLevelInformation, conn, typeof(dataTable$Geo))
      })
    }
    odbcCloseAll()
  }
  message("Done!")
  if(writeToDatabase) exportedFilesList$projectYear <- projectYear
  unique(exportedFilesList) #if there's more than one sumlev it'll return identical file list more than once hence the unique, fix it later if there's time
}

# This will run all other functions ---------------------------------------

main <- function() {
  # Function to run all others
  # @return None, run checking and processing functions and notify if all done correctly

  #### TODO CHECK IF THIS IS ALREADY DONE!!!!
  # Check if table_names existsts if yes, delete only for that year, and set  writeTableNames to append to existindatabase

  # set working directory to that of a script, it won't work in rstudio session hence the try
  # try(setwd(dirname(sys.frame(1)$ofile)), silent = FALSE)

  #******************************************************************************************************
  # check command line arguments first

  CMDargs <- commandArgs(trailingOnly = T)

  if(CMDargs[1] == '-c' && file.exists(CMDargs[2])) {
    pathToConfig <- CMDargs[2]
  } else {
    if(length(CMDargs) >0) message("Incorect parameters provided, use -c PATH to specify full path to config.yml!")
    message("Looking for config.yml in the same directory!")
    pathToConfig <- 'config.yml'
  }

  exportedFilesList <- data.frame()
  config <- yaml.load_file(pathToConfig)

  checkPrerequisites(config, pathToConfig)

  writeToDatabase <- config$writeToDatabase
  sourceDirectory <- config$sourceDirectory
  outputDirectory <- config$outputDirectory
  configDirectory <- config$configDirectory
  projectId <- config$projectId
  dbName <- config$dbName
  serverName <- config$server
  geoLevelInfo <- config$geoLevelInfo
  projectYear <- config$projectYear

  if(config$fileNamesList=='') {
    print("No fileNamesList file defined, looking for files in sourceDirectory!")
    tryCatch(inputFileNamesList <- list.files(path = sourceDirectory), error = function() stop("Can't find source data, check fileNamesList and sourceDirectory in config file!")) #TODO test this
  } else {
    inputFileNamesList <- read.csv(config$fileNamesList, stringsAsFactors = F, header = F)[,1]
  }

  if (!("trustedConnection" %in% names(config))) { # if parameter is not defined in config file than use default value
    trustedConn <- TRUE
  } else {
    trustedConn <- config$trustedConnection
  }

  if (!("tableNumberingStartsFrom" %in% names(config))) { # if parameter is not defined in config file than use default value
    counter <- 1
  } else {
    counter <- config$tableNumberingStartsFrom
  }


  user <- config$user
  password <- config$password

  prepareTableNames(projectYear, serverName, user, password, dbName, trustedConn)
  for(fileName in inputFileNamesList) {
    if(file.exists(paste0(sourceDirectory,fileName))) {
      message(paste(counter, ": Starting: ",fileName,sep=""))
      datasetId = substr(fileName, 1, gregexpr(pattern = '_', fileName[1])[[1]][1]-1)
      print(datasetId)
      exportedFilesList <- rbind(exportedFilesList, processFile(datasetId, fileName, counter, writeToDatabase, sourceDirectory, outputDirectory, configDirectory, projectId, dbName, serverName, geoLevelInfo, projectYear, user, password, trustedConn))
    } else {
      message(paste0('File ',fileName,' does not exist, table number will be increased by one anyway!'))
    }
    counter <- counter + 1
  }

  createMissingTables(config);
  if(writeToDatabase) writeTableNames(outputDirectory, exportedFilesList, dbName, serverName, user, password, trustedConn)
  print('All done!!!')
}


main()
stopCluster(myCluster)
