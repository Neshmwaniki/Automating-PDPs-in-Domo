
# clear environment

rm(list = ls())

# Libraries

library(tidyverse)

##### Set working directory

getwd()

### Set up connection to Domo

domo <- rdomo::Domo()


# domo <- rdomo::Domo(client_id = '****************',
#                     secret = '*******',
#                     domain = 'api.domo.com',
#                     scope = c('data', 'workflow'))#, 'dashboard'



start_time_f <- Sys.time()

# ### Domo ETL Data Source
# ds <- janitor::clean_names(domo$ds_get('****************')) #Read ALL Dataflows from DOMO
# ## make a copy
# datasets <- ds
# 
# ### filtering conditions
# filtered_datasets <- datasets %>% filter(column_name == 'country_id',
#                                   dataset_id != '*********************',
#                                   pdp_enabled != TRUE,
#                                   column_count > 0,
#                                   type != 'workbench-odbc',
#                                   !grepl('Fresh|Bringg|Uganda',name))
# 
# 
# #### 


#Retrieve a list of all datasets

df <- domo$ds_list(limit = 0, offset = 0, df_output = TRUE)

## Filter for Datasets that don't have the name fresh in them
# df2 <- df[grepl("Fresh",df$name),] 

#Filter out datasets for setting up pdp, ones with no schema and with pdp enabled

#currently the below filters for fresh datasets
# df2 <- df %>% filter(id != c("*******************") & columns != 0 & pdpEnabled != TRUE & grepl('Fresh',name)) #FreshDatasets

#currently the below filters for Non-fresh datasets
df2 <- df %>% filter(id != c("*******************") & columns != 0 & pdpEnabled != TRUE & !grepl('Fresh|Bringg|Uganda',name)) #shops_masterfile


# list_x_datasets <- filtered_datasets$dataset_id
list_x_datasets <- df2$id


length(list_x_datasets)

#Get the metadata to check for column names

x_list <- lapply(list_x_datasets, domo$ds_meta)


# Function to filter rows with any column containing 'country' in the 'name' of the 'schema'
filtered_list <- lapply(x_list, function(x) {
  # Extract the 'name' column from 'schema'
  schema_names <- sapply(x$schema$columns, function(col) col$name)
  # Check if any column name contains 'country_id' (case-insensitive)
  if (any(grepl("country_id", schema_names, ignore.case = TRUE))) {
    # If 'country' is found in column names, return the 'id' element only
    return(x$id)
  } else {
    # If 'country' is not found in column names, return NULL
    return(NULL)
  }
})


# Remove NULL elements from the filtered list
filtered_list <- filtered_list[!sapply(filtered_list, is.null)]

filtered_ids <- unlist(filtered_list)


# Output the filtered list with $id values only
print(filtered_ids)


# copy the pdps from the key dataset and implement in the above list of filtered datasets

# Fetch the existing_pdp list using the existing_dataset ID
existing_dataset <- "*******************"
existing_pdp <- domo$pdp_list(existing_dataset)

# Function to create PDPs for a list of dataset IDs
create_pdps <- function(dataset_ids, existing_pdp) {
    # Loop through each dataset ID in the list
  for (id in dataset_ids) {
    # Skip if the ID is NULL (no 'country' columns in the schema)
    if (!is.null(id)) {
      # Create a PDP for the new dataset using the existing PDP settings
      for (x in existing_pdp) {
        if (x$name != 'All Rows') {
          # Get the URL string for the PDP
          pdp <- domo$pdp_create(id, x)
          print(paste("PDP Created for dataset:", id, "and PDP name:", x$name))
        }
      }
    }
  }
}

# Call create_pdps function with filtered_ids and existing_pdp
create_pdps(filtered_ids, existing_pdp)

# List of PDP URLs to enable
pdp_urls_to_enable <- c(filtered_ids, existing_dataset)


# Enable the PDPs for a list of PDP URLs
enable_pdps <- function(pdp_urls) {
  for (url in pdp_urls) {
    if (length(url) == 1) {
      domo$pdp_enable(url, TRUE)
      print(paste("Enabled PDP:", url))
    } else {
      warning("Invalid PDP URL:", url)
    }
  }
}


# Call enable_pdps function with the list of PDP URLs
enable_pdps(pdp_urls_to_enable)

end_time_f <- Sys.time()

end_time_f - start_time_f

