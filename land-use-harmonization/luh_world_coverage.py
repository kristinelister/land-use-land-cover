import sys
import os
from osgeo import gdal, gdalconst, osr, gdal_array
from collections import OrderedDict
import numpy as np
import netCDF4 as nc
from matplotlib import pyplot as plt
import pandas as pd

#The purpose of this code is to find the global area coverage of each land cover category for Land-Use Harmonization version 2 data

DATA_DIR = 'LUH_data/data/'
NC_FILENAME = DATA_DIR+'baseline_states.nc'
STATIC_DATA_FILENAME = DATA_DIR+'staticData_quarterdeg.nc'
START_YEAR = 850

VARIABLES = ['urban','c3ann','c4ann','c3per','c4per','c3nfx','range','pastr','primf','secdf','primn','secdn']
NEW_VARIABLES = ['urban','crops','range','pastr','forest','nonforest']
NEW_VARIABLE_NAMES = ['Urban','Cropland','Rangeland','Pastureland','Forest','Nonforest']
META_VARIABLE = 'primf'

OUTPUT_CSV = DATA_DIR + 'LUH_baseline_global_area.csv'

TARGET_YEARS = np.arange(850,2016)


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
        
def getGlobalArea(var_data,cell_area,NDV):
    #Set no data value to 0
    var_data[var_data>=NDV] = 0
    #Multiply percent cover matrix by grid cell area matrix
    area_coverage = np.multiply(var_data,cell_area)
    #Sum over values to get total coverage
    area_sum = np.sum(area_coverage)
    return area_coverage,area_sum
   
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

#Create empty dataframe that will hold area coverage over all years
columns = ['Year']+NEW_VARIABLE_NAMES+['Total']
df = pd.DataFrame(columns=columns)

#Iterate through the years and calculate coverage sum
for index,year in enumerate(TARGET_YEARS):
    print(year)
    #Re-index year 
    year_index = year-START_YEAR
    #Get data for new variables from netcdf
    nc_data = GetNCDataByNewVariable(year_index,ysize,xsize,NDV,NC_FILENAME,NEW_VARIABLES)
    #Create empty row to be appended to dataframe
    df_row = np.zeros(len(NEW_VARIABLES)+2)
    #First entry is the year
    df_row[0] = year
    total = 0
    #For each variable find the area coverage
    for var_index, var in enumerate(NEW_VARIABLES):
        area_coverage,area_sum = getGlobalArea(nc_data[var_index,:,:],cell_area,NDV)
        #added 1 to index to reflect first entry is year
        df_row[var_index+1] = area_sum
        total = total + area_sum
    df_row[-1] = total
    #Insert into dataframe
    df.loc[index] = df_row

#Save to csv
df.to_csv(OUTPUT_CSV,index=False)