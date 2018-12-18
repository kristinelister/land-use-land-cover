import rasterio
from rasterio.mask import mask
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import mapping
write_zero_frequencies = True
show_plot = False
np.set_printoptions(suppress=True)

#The purpose of this program is to find the pixel count of each ESA Land Cover category
#Within ecoregions defined by RESOLVE
#Saves ecoregion pixel count by ESA Land Cover category to separate CSV's for each year

#This is the shapefile that contains the Resolve Ecoregions 
shape_fn = "Ecoregions2017/Ecoregions2017.shp"
output_dir = "ESA_Landcover/"
#Rasters that contain ESA Landcover that has been converted from UN land cover classification system to IPCC classification system
rasters = "esa_landcover_tifs/ESA_{year}_ipcc.tif"
#Output csv names for each year
outputs = output_dir + 'esa_lc_ecoregions_pixel_count_{year}.csv'


#Read in shapefile
shapefile = gpd.read_file(shape_fn)

#For each year
for i in np.arange(1992,2016):
    #Read in ESA landcover file for that year
    raster_fn = rasters.format(year=i)
    #Format output csv filename
    output_fn = outputs.format(year=i)
    with rasterio.open(raster_fn) as src:
        #Create empty dataframe
        df = pd.DataFrame(columns=['OBJECTID','ECO_NAME','BIOME_NAME','REALM','No_data','Agriculture','Forest','Grassland','Wetland','Settlement','Shrubland','Sparse_vegetation','Bare_area','Water','Snow_ice','Total'])
        #For each ecoregion in shapefile
        for index, row in shapefile.iterrows():
            #Read in geometry, object id, ecoregion name, biome name, and realm
            ecoregion = row['geometry']
            eco_id = row['OBJECTID']
            eco_name = row['ECO_NAME']
            biome_name = row['BIOME_NAME']
            realm = row['REALM']
            #Print statement for tracking progress
            #print('Year: '+str(i)+' ID: '+ str(eco_id) + ', Ecoregion: '+eco_name)
            #Skips Antarctica because it was too large and did not match to a biome
            if eco_id != 207:
                # transform to GeJSON format
                mapped_geom = [mapping(ecoregion)]
                
                # extract the raster values values within the polygon, pixels are included if their centroid lays within the polygon
                out_image, out_transform = mask(src, mapped_geom, crop=True)

                # extract the values of the masked array
                data = out_image[0]
                
                #Sum pixels that match categories
                no_data = (data==-1).sum()
                agriculture = (data==1).sum()
                forest = (data==2).sum()
                grassland = (data==3).sum()
                wetland = (data==4).sum()
                settlement = (data==5).sum()
                shrubland = (data==6).sum()
                sparse_veg = (data==7).sum()
                bare_area = (data==8).sum()
                water = (data==210).sum()
                snow = (data==220).sum()
                #Find total
                total = (data>0).sum()+(data==-1).sum()
                #Save to dataframe
                df.loc[index]=[eco_id,eco_name,biome_name,realm,no_data,agriculture,forest,grassland,wetland,settlement,shrubland,sparse_veg,bare_area,water,snow,total]
        #Save to csv
        df.to_csv(output_fn,encoding = 'utf-8')