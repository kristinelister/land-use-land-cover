import rasterio
from rasterio.mask import mask
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import mapping
from matplotlib import pyplot as plt
from rasterio.plot import show
from matplotlib.colors import ListedColormap, BoundaryNorm
import matplotlib.colors as colors

#The purpose of this code is to find the global area coverage of each land cover category for ESA landcover

#Set file names
output_dir = "ESA_Landcover/"
rasters = "esa_landcover_tifs/ESA_{year}_ipcc.tif"
output_csv = output_dir+'esa_global_area.csv'

#Set variable names
VARIABLES = ['Agriculture','Forest','Grassland','Wetland','Settlement','Shrubland','SparseVegetation','BareArea','Water','SnowIce']
#Set target years
YEARS = np.arange(1992,2016)

#Create empty dataframe
columns = ['Year']+VARIABLES
df = pd.DataFrame(columns=columns)

#Iterate through years to find area coverage of land cover classes for that year
for i,year in enumerate(YEARS):
    print(year)
    raster_fn = rasters.format(year=year)
    #Create empty array for new row
    new_row = np.zeros(len(columns))
    #First entry is year
    new_row[0]=year
    with rasterio.open(raster_fn) as src:
        data = src.read(1)
        data = np.array(data)
        print(np.shape(data))
        print('opened data for year {year}'.format(year=year))
        #Each pixel is 300 m x 300 m
        area_factor = 300.0*300.0
        new_row[1] = np.sum((data==1))*area_factor #agriculture
        new_row[2] = np.sum((data==2))*area_factor #forest
        new_row[3] = np.sum((data==3))*area_factor #grassland
        new_row[4] = np.sum((data==4))*area_factor #wetland
        new_row[5] = np.sum((data==5))*area_factor #settlement
        print('Halfway there')
        new_row[6] = np.sum((data==6))*area_factor #shrubland
        new_row[7] = np.sum((data==7))*area_factor #sparse vegetation
        new_row[8] = np.sum((data==8))*area_factor #barea area
        new_row[9] = np.sum((data==210))*area_factor #water
        new_row[10] = np.sum((data==220))*area_factor #snow
    #Save to dataframe
    print(new_row)
    df.loc[i] = new_row
        
#Save to csv
df.to_csv(output_csv,index=False)