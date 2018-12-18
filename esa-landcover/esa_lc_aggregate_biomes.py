import numpy as np
import pandas as pd
from matplotlib import pyplot

pd.options.display.max_colwidth = 1000
np.set_printoptions(suppress=True)

#The purpose of this program is to aggregate the pixel counts of each ecoregion into unique biome, realm pairs; unique biomes; and unique realms
#Also finds the percent coverage as well for each ecoregion; unique biome, realm pairs; unique biomes; and unique realms

#Set directory and output CSV names
direc = 'ESA_Landcover'
input_csv = direc + 'esa_lc_all_year_ecoregions_pixel_count.csv'

all_perc_csv = direc+'esa_lc_all_year_ecoregions_percent.csv'
biome_realm_csv = direc + 'esa_lc_biome_realm_pixel_count.csv'
biome_csv = direc + 'esa_lc_biome_pixel_count.csv'
realm_csv = direc + 'esa_lc_realm_pixel_count.csv'
biome_realm_perc_csv = direc + 'esa_lc_biome_realm_percent.csv'
biome_perc_csv = direc + 'esa_lc_biome_percent.csv'
realm_perc_csv = direc + 'esa_lc_realm_percent.csv'

#Number of years
num_years = 2016-1992
#Land cover column names
lc_names = ['Agriculture','Forest','Grassland','Wetland','Settlement','Shrubland','Sparse_vegetation','Bare_area','Water','Snow_ice']
#Read in dataframe
in_df = pd.read_csv(input_csv,header=0,index_col=0)

#1. CONVERT PIXEL COUNTS TO PERCENT COVER FOR EACH ECOREGION
#The purpose of this section of code is to find the percent coverage of each land cover class over the years for each of the ecoregions
#Copy over dataframe
perc_df = in_df.copy()
#Iterate over dataframe
for index, row in in_df.iterrows():
   total = row['Total']
   for lc in lc_names:
       for i,year in enumerate(np.arange(1992,2016)):
           column_name = lc+'_'+str(year)
           lc_val = perc_df.at[index,column_name]
           perc_val = lc_val/total
           perc_df.at[index,column_name] = perc_val
perc_df.to_csv(all_perc_csv)

#2. PIXEL COUNTS FOR UNIQUE BIOME, REALM PAIRS
#The purpose of this section of code is to find the land cover information for unique biome, realm pairs
#Get unique biome, realm pairs
biomes_realms = in_df[['BIOME_NAME','REALM']]
biomes_realms = biomes_realms.drop_duplicates()
#Copy over biome and realm values
biome_realm_df = pd.DataFrame(columns=['Biome','Realm'])
biome_realm_df['Biome'] = biomes_realms['BIOME_NAME'].tolist()
biome_realm_df['Realm'] = biomes_realms['REALM'].tolist()
biome_realm_df['Total'] = 0.0

#2a. SUM PIXEL COUNTS FOR EACH BIOME, REALM PAIR
#The purpose of this section of code is to sum the pixel counts of each year and land cover class over the ecoregions that match
#   the biome, realm pairs as above
#Iterate over the biome,realm pairs of the biome_realm_df and fill in land cover values
for biome_index,biome_row in biome_realm_df.iterrows():
    #Get subset of dataframe of ecoregions that match the biome, realm pair
    temp_df = in_df[(in_df['BIOME_NAME'] == biome_row['Biome']) & (in_df['REALM'] == biome_row['Realm'])]
    biome_realm_df.at[biome_index,'Total'] = np.sum(np.array(temp_df['Total'].tolist()))
    #Iterate over land cover types
    for lc in lc_names:
        #Iterate over years
        for i, year in enumerate(np.arange(1992,2016)):
            #Get column name
            column_name = lc+'_'+str(year)
            #Get array of pixel counts for that year and land cover for ecoregions that match that biome and realm
            lc_year_array = np.array(temp_df[column_name].tolist())
            #Sum over pixel counts
            lc_year_sum = np.sum(lc_year_array)
            #Save to dataframe
            biome_realm_df.at[biome_index,column_name] = lc_year_sum
#Save dataframe to csv
biome_realm_df.to_csv(biome_realm_csv)


#2b. FIND PERCENT COVERAGE OF EACH LAND COVER TYPE FOR EACH UNIQUE BIOME, REALM PAIRS
#The purpose of this section of code is to find the percent coverage of each land cover class for the 
#   unique biome, realm pairs
#Copy over dataframe
biome_realm_perc_df = biome_realm_df.copy()
#Iterate over the biome, realm pairs
for index, row in biome_realm_perc_df.iterrows():
    #Save total
    total = row['Total']
    #Iterate through lc names
    for lc in lc_names:
        #Iterate through years
        for year in np.arange(1992,2016):
            #Find percent land cover for that land cover type and year
            column_name = lc+'_'+str(year)
            lc_year_val = biome_realm_perc_df.at[index,column_name]
            lc_year_perc = lc_year_val/total
            #Save back to dataframe
            biome_realm_perc_df.at[index,column_name] = lc_year_perc
biome_realm_perc_df.to_csv(biome_realm_perc_csv)



#3. PIXEL COUNTS FOR BIOMES
#The purpose of this section of code is to find the pixel counts for each biome
#Get unique biome names
biomes = in_df['BIOME_NAME']
biomes = biomes.drop_duplicates()
#Copy over biome values
biome_df = pd.DataFrame(columns=['Biome'])
biome_df['Biome'] = biomes.tolist()
#Save empty column for total
biome_df['Total'] = 0.0

#3a. SUM PIXEL COUNTS FOR EACH BIOME
#The purpose of this section of code is to sum the pixel counts of each year and land cover class over the ecoregions that match the biomes above
#Iterate over the biomes of the biome_df and fill in land cover values
for biome_index,biome_row in biome_df.iterrows():
    #Get subset of dataframe of ecoregions that match the biome
    temp_df = in_df[in_df['BIOME_NAME'] == biome_row['Biome']]
    biome_df.at[biome_index,'Total'] = np.sum(np.array(temp_df['Total'].tolist()))
    #Iterate over land cover types
    for lc in lc_names:
        #Iterate over years
        for i, year in enumerate(np.arange(1992,2016)):
            #Get column name
            column_name = lc+'_'+str(year)
            #Get array of pixel counts for that year and land cover for ecoregions that match that biome 
            lc_year_array = np.array(temp_df[column_name].tolist())
            #Sum over pixel counts
            lc_year_sum = np.sum(lc_year_array)
            #Save land cover sum into dataframe
            biome_df.at[biome_index,column_name] = lc_year_sum
#Save dataframe to csv
biome_df.to_csv(biome_csv)


#3b. FIND PERCENT COVERAGE OF EACH LAND COVER TYPE FOR EACH UNIQUE BIOME
#The purpose of this section of code is to find the percent coverage of each land cover class for the biomes
#Copy over dataframe
biome_perc_df = biome_df.copy()
#Iterate over the biomes
for index, row in biome_perc_df.iterrows():
    #Save total
    total = row['Total']
    #Iterate through lc names
    for lc in lc_names:
        #Iterate through years
        for year in np.arange(1992,2016):
            #Find percent land cover for that land cover type and year
            column_name = lc+'_'+str(year)
            lc_year_val = biome_perc_df.at[index,column_name]
            lc_year_perc = lc_year_val/total
            #Save back to dataframe
            biome_perc_df.at[index,column_name] = lc_year_perc
biome_perc_df.to_csv(biome_perc_csv)

#4. PIXEL COUNTS FOR Realms
#The purpose of this section of code is to find the pixel counts for each realm
#Get unique realm names
realms = in_df['REALM']
realms = realms.drop_duplicates()
#Copy over realm values
realm_df = pd.DataFrame(columns=['Realm'])
realm_df['Realm'] = realms.tolist()
#Save empty column for total
realm_df['Total'] = 0.0

#4a. SUM PIXEL COUNTS FOR EACH REALM
#The purpose of this section of code is to sum the pixel counts of each year and land cover class over the ecoregions that match the realms above
#Iterate over the realms of the realm_df and fill in land cover values
for realm_index,realm_row in realm_df.iterrows():
    #Get subset of dataframe of ecoregions that match the realm
    temp_df = in_df[in_df['REALM'] == realm_row['Realm']]
    realm_df.at[realm_index,'Total'] = np.sum(np.array(temp_df['Total'].tolist()))
    #Iterate over land cover types
    for lc in lc_names:
        #Iterate over years
        for i, year in enumerate(np.arange(1992,2016)):
            #Get column name
            column_name = lc+'_'+str(year)
            #Get array of pixel counts for that year and land cover for ecoregions that match that realm 
            lc_year_array = np.array(temp_df[column_name].tolist())
            #Sum over pixel counts
            lc_year_sum = np.sum(lc_year_array)
            #Save land cover sum into dataframe
            realm_df.at[realm_index,column_name] = lc_year_sum
#Save dataframe to csv
realm_df.to_csv(realm_csv)


#4b. FIND PERCENT COVERAGE OF EACH LAND COVER TYPE FOR EACH UNIQUE REALM
#The purpose of this section of code is to find the percent coverage of each land cover class for the realms
#Copy over dataframe
realm_perc_df = realm_df.copy()
#Iterate over the realms
for index, row in realm_perc_df.iterrows():
    #Save total
    total = row['Total']
    #Iterate through lc names
    for lc in lc_names:
        #Iterate through years
        for year in np.arange(1992,2016):
            #Find percent land cover for that land cover type and year
            column_name = lc+'_'+str(year)
            lc_year_val = realm_perc_df.at[index,column_name]
            lc_year_perc = lc_year_val/total
            #Save back to dataframe
            realm_perc_df.at[index,column_name] = lc_year_perc
realm_perc_df.to_csv(realm_perc_csv)

