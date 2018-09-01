# @file main
#
# Copyright 2017 Observational Health Data Sciences and Informatics
#
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# @author Observational Health Data Sciences and Informatics
# @author Amy Matcho
# @author Chris Knoll
# @author Ajit Londhe
# @author Gennadiy Anisimov



#' Init Tables
#'
#' @details Initalizes lookup and result tables for pregnancy algorithm. Uses Bulk upload in InsertTable if possible.
#'
#' @param connectionDetails        An R object of type ConnectionDetails (details for the function that contains server info, database type, optionally username/password, port)
#' @param resultsDatabaseSchema    Fully qualified name of database schema that we can write final results to. Default is cdmDatabaseSchema. 
#'                                 On SQL Server, this should specifiy both the database and the schema, so for example, on SQL Server, 'cdm_results.dbo'.
#' @return none
#' 
#' @export
init <- function(connectionDetails, resultsDatabaseSchema)
{
  connection <- DatabaseConnector::connect(connectionDetails)
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "initTables.sql", 
                                           packageName = "PregnancyAlgorithm", 
                                           dbms = connectionDetails$dbms, warnOnMissingParameters = FALSE,
                                           resultsDatabaseSchema = resultsDatabaseSchema)
  
  DatabaseConnector::executeSql(connection = connection, sql = sql)
  DatabaseConnector::disconnect(connection = connection)
  writeLines("Pregnancy episodes table created");
  
  for (file in list.files(path = paste(system.file(package = 'PregnancyAlgorithm'), "csv/", sep = "/"), 
                          full.names = TRUE)) {
    
    connection <- DatabaseConnector::connect(connectionDetails)   
    df <- read.csv(file = file, header = TRUE, stringsAsFactors = FALSE)
    
    DatabaseConnector::insertTable(connection = connection, 
                                   tableName = sprintf("%1s.%2s", resultsDatabaseSchema,
                                                       tools::file_path_sans_ext(basename(file))), 
                                   data = df, 
                                   dropTableIfExists = TRUE, 
                                   createTable = TRUE, useMppBulkLoad = TRUE)
  }

  
  writeLines("Pregnancy algorithm tables initalized.")
}

#' Clean Tables
#'
#' @details                        Removes all tables related to the pregnancy algorithm
#' @param connectionDetails        An R object of type ConnectionDetails (details for the function that contains server info, database type, optionally username/password, port)
#' @param resultsDatabaseSchema    Fully qualified name of database schema that we can write final results to. Default is cdmDatabaseSchema. 
#' @return none
#'
#' @export
clean <- function(connectionDetails, resultsDatabaseSchema)
{
  connection <- DatabaseConnector::connect(connectionDetails)
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "clean.sql", 
                                           packageName = "PregnancyAlgorithm", 
                                           dbms = connectionDetails$dbms, 
                                           warnOnMissingParameters = FALSE,
                                           resultsDatabaseSchema = resultsDatabaseSchema)
  
  DatabaseConnector::executeSql(connection = connection, sql = sql)
  DatabaseConnector::disconnect(connection = connection)
  writeLines("Pregnancy algorithm tables removed");
}

#' Execute Algorithm
#'
#' @details
#' Executes the pregnancy identification algorithm.  init() must be called before executing this.
#' 
#' @param connectionDetails        An R object of type ConnectionDetails (details for the function that contains server info, database type, optionally username/password, port)
#' @param cdmDatabaseSchema        Fully qualified name of database schema that holds the CDM.
#' @param resultsDatabaseSchema    Fully qualified name of database schema that we can write final results to. Default is cdmDatabaseSchema. 
#' @param sqlOnly                  Execute in SQL Only mode?
#' @return
#' none
#'
#' @export
execute <- function(connectionDetails, 
                    cdmDatabaseSchema, 
                    resultsDatabaseSchema = cdmDatabaseSchema, 
                    sqlOnly = FALSE)
{
  executeSqlSteps <- function(sqlScript)
  {
    sql <- SqlRender::loadRenderTranslateSql(sqlFilename = sqlScript, 
                                             packageName = "PregnancyAlgorithm", 
                                             dbms = connectionDetails$dbms,
                                             warnOnMissingParameters = FALSE,
                                             resultsDatabaseSchema = resultsDatabaseSchema,
                                             cdmDatabaseSchema = cdmDatabaseSchema)
    if (sqlOnly)
    {
      write(x = sql, file = paste("output", basename(sqlScript), sep = "/"), append = TRUE)
    }
    else
    {
      DatabaseConnector::executeSql(connection = connection, sql = sql)
    }
  }

  executeLoopedSql <- function(stepNum, sqlBreak, sqlBreakResult, sqlLoopPreBreak, sqlLoopPostBreak, tablesToClean = c())
  {
    while (1)
    {
      executeSqlSteps(sqlLoopPreBreak)
      if (DatabaseConnector::querySql(connection, sqlBreak) == sqlBreakResult)
      {
        break
      }
      executeSqlSteps(sqlLoopPostBreak)
    }
    for (table in tablesToClean)
    {
      objName <- ifelse(startsWith(table, "#"), paste0("tempdb..", table), table)
      sql <- paste0("IF OBJECT_ID('", objName, "', 'U') IS NOT NULL\r\ndrop table ", table, ";")
      sql <- SqlRender::translateSql(sql = sql, targetDialect = connectionDetails$dbms)$sql
      if (sqlOnly)
      {
        write(x = sql, file = paste("output", basename(sqlScript), sep = "/"), append = TRUE)
      }
      else
      {
        DatabaseConnector::executeSql(connection = connection, sql = sql)
      }
    }
  }

  connection <- DatabaseConnector::connect(connectionDetails)

  #### execute algorithm ####

  # Step 1 Script
  writeLines("Running Step 1")
  executeSqlSteps("algorithm/step1.sql")

  # Steps 2 through 8
  steps <- c(2,3,5:8)
  firstOutcomeEvent <- SqlRender::renderSql("@resultsDatabaseSchema.FirstOutcomeEvent",
                                 resultsDatabaseSchema = resultsDatabaseSchema)$sql
  sqlBreak <- paste0("select count(*) from ", firstOutcomeEvent)
  for (step in steps)
  {
    writeLines(paste0("Running Step ", step))
    executeSqlSteps(paste0("algorithm/step", step, "_0.sql"))
    executeLoopedSql(stepNum = step, sqlBreak = sqlBreak,
                     sqlBreakResult = 0, sqlLoopPreBreak = paste0("algorithm/step", step, "_1.sql"),
                     sqlLoopPostBreak = paste0("algorithm/step", step, "_2.sql"),
                     c("#PregnancyEvents", firstOutcomeEvent))
  }

  # Step 9 Script
  writeLines("Running Step 9")
  executeSqlSteps("algorithm/step9.sql")

  DatabaseConnector::disconnect(connection)
  writeLines("Pregnancy episodes generated.")
}

