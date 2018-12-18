import numpy as np
import pandas as pd

pd.options.display.max_colwidth = 100
np.set_printoptions(suppress=True)

#The purpose of this program is to merge individual pixel count year csv's into one csv


#Read in data
direc = 'ESA_Landcover/'
input_csv = direc + 'esa_lc_ecoregions_pixel_count_{year}.csv'
output_csv = direc + 'esa_lc_all_year_ecoregions_pixel_count.csv'

#List of Land Cover Classes to iterate over
lc_names = ['Agriculture','Forest','Grassland','Wetland','Settlement','Shrubland','Sparse_vegetation','Bare_area','Water','Snow_ice','Total']

#Number of years
num_years = 2016-1992
#Read in one of the CSV's to get the constant values
temp_df = pd.read_csv(input_csv.format(year=1992),header=0)
#Get those constant values like object id and eco name into the dataframe
df = temp_df[['OBJECTID','ECO_NAME','BIOME_NAME','REALM']].copy()
#Initialize total pixel count column
df['Total'] = 0.

#Now iterate through the year CSVs and insert that year's data into the dataframe
for i,year in enumerate(np.arange(1992,2016)):
    #Read in CSV
    year_df = pd.read_csv(input_csv.format(year=year),header=0)
    #For each row of the CSV
    for index, year_row in year_df.iterrows():
        #Get the object id of the row
        objid = year_row['OBJECTID']
        #Get the index of the dataframe corresponding to that object id
        df_index = df.index[df['OBJECTID'] == objid].tolist()
        df_index = df_index[0]
        #Iterate through LC Names
        for lc in lc_names:
            #Insert landcover pixel count into the corresponding column for that year and landcover
            df.at[df_index,lc+'_'+str(year)] = year_row[lc]

#Check that the total pixel count through the years does not change
#Get column names of total pixel count through the yeras
total_column_names = [""]*num_years
for i,year in enumerate(np.arange(1992,2016)):
    total_column_names[i] = 'Total_'+str(year)
#Iterate over dataframe to find total pixel counts through the years
for index, row in df.iterrows():
    total_row = np.array(row[total_column_names].tolist())
    #Find unique total counts
    total_unique = np.unique(total_row)
    #If there is only one, then the pixel count is consistent through the years, save that number to 'Total' column
    if len(total_unique) == 1:
        df.at[index,'Total'] = total_unique[0]
    #If not we have a problem
    else:
        print('ERROR: TOTAL PIXEL COUNT NOT CONSISTENT ACROSS YEARS')
        break
#Drop old total columns
df = df.drop(columns=total_column_names)
#Save to CSV!
df.to_csv(output_csv)