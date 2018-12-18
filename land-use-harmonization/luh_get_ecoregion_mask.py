import geopandas as gpd
import pandas as pd
import numpy as np
import shapely.geometry as geo
from shapely.geometry import mapping
from matplotlib import pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm
import matplotlib.colors as colors
import netCDF4 as nc

#The purpose of this code is to get masks for Land-Use Harmonization data for ecoregions
#These masks are saved to numpy array files that can be read in again later
#There is one mask for each ecoregion


DATA_DIR = "LUH_data/data/"
mask_filename = "ecoregions_mask/LUH_ecoregions_mask"
shapefile_name = "Ecoregions2017/Ecoregions2017.shp"
netcdf_fn = "LUH_data/data/baseline_states.nc"

def get_coordinates(netcdf_name):
    '''
    Function to get latitude and longitude coordinates from netcdf and size of data
    '''
    #Initialize
    lat = None
    lon = None
    lon_shape = None
    lat_shape = None
    with nc.Dataset(netcdf_name) as src:
        #Pull out latitude and longitude coordinates
        lat = src.variables['lat'][:]
        lon = src.variables['lon'][:]
    #Get shape of latitude and longitude vectors
    lon_shape = len(lon)
    lat_shape = len(lat)
    #Returns latitude and longitude as [longitude, latitude] pairs in a long list
    coords = [[longitude,latitude] for latitude in lat for longitude in lon]
    coords = np.array(coords)
    #Reshape coordinates so they are in the same shape as data
    coords = coords.reshape((lon_shape,lat_shape,2))
    return coords, lon_shape,lat_shape

def get_mask(geometry,coords,lon_shape,lat_shape,obj_id):
    #Initialize empty array to save mask
    ecoregion_mask = np.zeros((lon_shape,lat_shape))
    #Iterate through rows and columns
    for i in np.arange(lon_shape):
        for j in np.arange(lat_shape):
            lon = coords[i,j,0]
            lat = coords[i,j,1]
            print('ID: {ID}, Latitude: {lat}, Longitude: {lon}'.format(ID=obj_id,lat=lat,lon=lon))
            #Find whether point is in ecoregion
            ecoregion_mask[i,j]= geometry.contains(geo.Point(lon,lat))
    return ecoregion_mask


#Get coordinates, longitude shape, and latitude shape from netCDF
coords, lon_shape, lat_shape = np.array(get_coordinates(netcdf_fn))
#Read shapefile
shapefile = gpd.read_file(shapefile_name)

for index,row in shapefile.iterrows():
    #Get object id
    obj_id = row['OBJECTID']
    print(obj_id)
    #Need to skip the Antarctica shape because it is too big, it is also not classified by Resolve Ecoregions
    if obj_id != 207 and obj_id in [705]:
        #Get geometry
        ecoregion = row['geometry']
        #Get mask
        mask = get_mask(ecoregion,coords,lon_shape,lat_shape,obj_id)
        #Save mask to file to be read later
        np.save(DATA_DIR+mask_filename+'_'+str(int(obj_id)),mask)
        
