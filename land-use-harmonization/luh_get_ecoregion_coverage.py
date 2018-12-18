import sys
import os
from osgeo import gdal, gdalconst, osr, gdal_array
from collections import OrderedDict
import netCDF4 as nc
import rasterio
from rasterio.mask import mask
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import mapping
np.set_printoptions(suppress=True)

#The purpose of this code is to find the area coverage of each land use category of Land-Use Harmonization in the RESOLVE ecoregions


DATA_DIR = "LUH_data/data/"
NC_FILENAME = DATA_DIR+'baseline_states.nc'
STATIC_DATA_FILENAME = DATA_DIR+'staticData_quarterdeg.nc'
SHAPEFILE_NAME = "Ecoregions2017/Ecoregions2017.shp"

MASK_FILENAME = DATA_DIR + "ecoregions_mask/LUH_ecoregions_mask_{id}.npy"


TARGET_YEARS = np.arange(850,2016)
START_YEAR = 850

VARIABLES = ['urban','c3ann','c4ann','c3per','c4per','c3nfx','range','pastr','primf','secdf','primn','secdn']
NEW_VARIABLES = ['urban','crops','range','pastr','forest','nonforest']
NEW_VARIABLE_NAMES = ['Urban','Cropland','Rangeland','Pastureland','Forest','Nonforest']
META_VARIABLE = 'primf'

OUTPUT_CSV = DATA_DIR + 'csv/LUH_baseline_ecoregion_area_{year}.csv'

def get_mask(mask_filename):
    return np.load(mask_filename)
    
def GetnetCDFGlobalMetaData(in_filename):
    """
    Function to read global metadata of netcdf file
    """
    with nc.Dataset(in_filename) as src:
        return src.__dict__

def GetnetCDFVariableMetaData(in_filename,var_name):
    """
    Function to read variable metadata of netcdf file
    """
    with nc.Dataset(in_filename) as src:
        return src[var_name].__dict__

def GetnetCDFInfobyName(in_filename,var_name):
    """
    Function to read the original file's projection
    """
    #Open netCDF file
    src_ds = gdal.Open(in_filename)
    if src_ds is None:
        print("Open failed")
        sys.exit()
    if len(src_ds.GetSubDatasets()) > 1:
        #If exists more than one var in the NetCDF
        subdataset = 'NETCDF:"'+in_filename+'":'+var_name
        src_ds_sd = gdal.Open(subdataset)
        
        #begin to read info of the named variable
        NDV = src_ds_sd.GetRasterBand(1).GetNoDataValue()
        xsize = src_ds_sd.RasterXSize
        ysize = src_ds_sd.RasterYSize
        GeoT = src_ds_sd.GetGeoTransform()
        Projection = osr.SpatialReference()
        #Projection.ImportFromWkt(src_ds_sd.GetProjectionRef())
        Projection.ImportFromEPSG(4326)
        
        #Close the subdataset and the whole dataset
        src_ds_sd = None
        src_ds = None
        return NDV, xsize, ysize, GeoT, Projection
        

def GetnetCDFDataByName(in_filename,var_name,index=0):
    '''
    Reads data for a specified year from netCDF
    '''
    with nc.Dataset(in_filename) as src:
        return src.variables[var_name][index,:,:]
    
def GetStaticnetCDFDataByName(in_filename,var_name):
    '''
    Reads data for a specified year from netCDF
    '''
    with nc.Dataset(in_filename) as src:
        return src.variables[var_name][:,:]
        
def GetNCDataByNewVariable(year_index,ysize,xsize,NDV,NC_FILENAME=NC_FILENAME,NEW_VARIABLES=NEW_VARIABLES):
    '''
    Aggregate original variables to desired ones
    '''
    #Get data for variables and aggregate when necessary
    nc_data = np.zeros((len(NEW_VARIABLES),ysize,xsize))
    for var_index, var in enumerate(NEW_VARIABLES):
        #If not in the aggregating variables (crops, forest, nonforest) just keep variable
        temp_variables = [var]
        #Otherwise aggregate
        if var == 'crops':
            temp_variables = ['c3ann','c4ann','c3per','c4per','c3nfx']
        elif var == 'forest':
            temp_variables = ['primf','secdf']
        elif var == 'nonforest':
            temp_variables = ['primn','secdn']
        #"Aggregating"
        for temp_var in temp_variables:
            nc_data[var_index] = nc_data[var_index]+ GetnetCDFDataByName(NC_FILENAME,temp_var,index=year_index).data
        #Make NDV values uniform
        nc_data[nc_data>=NDV] = NDV
    return nc_data
    
def getEcoregionArea(var_data,cell_area,NDV,ecoregion_mask):
    #Set no data value to 0
    var_data[var_data>=NDV] = 0
    #Multiply cell_area by ecoregion_mask
    masked_area = np.multiply(cell_area,ecoregion_mask)
    #Multiply percent cover matrix by grid cell area matrix
    masked_area_coverage = np.multiply(var_data,masked_area)
    #Sum over values to get total coverage
    area_sum = np.sum(masked_area_coverage)
    return masked_area_coverage,area_sum
    
#Get static data for LUH
#Get netcdf metadata
NDV, xsize, ysize, GeoT, Projection = GetnetCDFInfobyName(NC_FILENAME,META_VARIABLE)
#Get area per grid cell
cell_area = GetStaticnetCDFDataByName(STATIC_DATA_FILENAME,'carea')
#Get ice/water fraction of cell, this is the amount of the grid cell covered in ice and water
ice_water_fraction = GetStaticnetCDFDataByName(STATIC_DATA_FILENAME,'icwtr')
#Multiply grid cell area by 1 - fraction of cell covered in ice and water to get
#   area of grid cell that is terrestrial
cell_area = np.multiply(cell_area,1-ice_water_fraction)

#Iterate through the years and calculate coverage sum
for index,year in enumerate(TARGET_YEARS):
    
    #Re-index year 
    year_index = year-START_YEAR
    #Get data for new variables from netcdf
    nc_data = GetNCDataByNewVariable(year_index,ysize,xsize,NDV,NC_FILENAME,NEW_VARIABLES)
    
    #Create empty dataframe that will hold area coverage over all ecoregions for that year
    columns = ['OBJECTID','ECO_NAME','BIOME_NAME','REALM','Year']+NEW_VARIABLE_NAMES+['Total']
    df = pd.DataFrame(columns=columns)
    shapefile = gpd.read_file(SHAPEFILE_NAME)
    print('Year: {year}'.format(year=year))
    for index,row in shapefile.iterrows():
        obj_id = row['OBJECTID']
        if obj_id != 207:
            eco_name = row['ECO_NAME']
            biome_name = row['BIOME_NAME']
            realm = row['REALM']
            mask = get_mask(MASK_FILENAME.format(id=int(obj_id)))
            mask = np.reshape(mask,(ysize,xsize))
            #Create empty row to be appended to dataframe
            df_row = np.zeros(len(NEW_VARIABLES)+1)
            total = 0
            #For each variable find the area coverage
            for var_index, var in enumerate(NEW_VARIABLES):
                area_coverage,area_sum = getEcoregionArea(nc_data[var_index,:,:],cell_area,NDV,mask)
                df_row[var_index] = area_sum
                total = total+ area_sum
            df_row[-1] = total
            #Insert into dataframe
            keys = columns
            values = [obj_id, eco_name, biome_name, realm, year] + df_row.tolist()
            dictionary = dict(zip(keys, values))
            df = df.append(dictionary, ignore_index=True)

    #Save to csv
    df.to_csv(OUTPUT_CSV.format(year=year),index=False,encoding='utf-8')


