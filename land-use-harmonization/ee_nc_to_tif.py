import sys
import os
from osgeo import gdal, gdalconst, osr, gdal_array
from collections import OrderedDict
import numpy as np
import netCDF4 as nc
import rasterio
from matplotlib import pyplot as plt
import datetime
import logging
import subprocess

#The purpose of this code is to upload data from Land-Use Harmonization version 2 to google earth engine
#The original netcdf's are converted to geotiffs and uploaded to earth engine

#Before running script make sure to authenticate google cloud service and earth engine
#Commands:
#> gsutil auth login
#> earthengine authenticate

#Before uploading assets, make sure to create folder and set to public
#Commands:
#> earthengine create folder (folder name)
#> earthengine acl set public (folder name)
#> earthengine create collection (collection name)
#> earthengine acl set public (collection name)

#EE_FOLDER = 'users/resourcewatchlandcover/LandUseHarmonization/Historical/Baseline'
EE_FOLDER = 'LandUseHarmonization/Projection/REMINDMAGPIE'
EE_COLLECTION = EE_FOLDER+ '/States'
GS_FOLDER = 'LandUseHarmonization'
GS_BUCKET = 'gs://upload_bucket/'

DATA_DIR = 'LUH_data/data/'
NC_FILENAME = DATA_DIR+'REMIND_states.nc'

VARIABLES = ['urban','c3ann','c4ann','c3per','c4per','c3nfx','range','pastr','primf','secdf','primn','secdn','secmb','secma']
META_VARIABLE = 'primf'

PAUSE_FOR_OVERLOAD = True
NUM_ASSETS_AT_ONCE = 50
START_YEAR = 2015


TARGET_YEARS = np.arange(2015,2101)#[850,1000]#,1200,1400,1600,1800,1900,2000,2015]

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
    
    
def create_geotiff(out_name,Array,NDV,xsize,ysize,GeoT,Projection):
    """
    Creates new GeoTiff from array
    """
    DataType = gdal_array.NumericTypeCodeToGDALTypeCode(Array.dtype)
    if type(DataType)!=np.int:
        if DataType.startswith('gdal.GDT_') == False:
            DataType=eval('gdal.GDT_'+DataType)
    NewFileName = out_name+'.tif'
    num_bands = Array.shape[0]
    
    #create a driver
    driver = gdal.GetDriverByName('GTiff')
    DataSet = driver.Create(NewFileName, xsize, ysize, num_bands,DataType)
    DataSet.SetGeoTransform(GeoT)
    DataSet.SetProjection(Projection.ExportToWkt())
    
    #Write data to geotiff
    for i in range(0,num_bands):
        DataSet.GetRasterBand(i+1).WriteArray(Array[i])
        DataSet.GetRasterBand(i+1).SetNoDataValue(NDV)
    DataSet.FlushCache()
    return NewFileName
    
    
def formatDate(date):
    '''Format date as ms since last epoch'''
    if isinstance(date, int):
        return date
    seconds = (date - datetime.datetime.utcfromtimestamp(0)).total_seconds()
    return int(seconds * 1000)
    
        
#Get global metadata properties to be saved in properties of the image collection
image_collection_properties = GetnetCDFGlobalMetaData(NC_FILENAME)

#Set these metadata values as properties of the image collection
for key, value in image_collection_properties.items():
    string = key+'='+value
    cmd = ['earthengine', 'asset', 'set', '-p', string, EE_COLLECTION]
    subprocess.call(cmd)
    
#Get metadata properties of the netcdf variables
NDV, xsize, ysize, GeoT, Projection = GetnetCDFInfobyName(NC_FILENAME,META_VARIABLE)
    
#Create empty list to save task ID's
task_ids = ['']*len(TARGET_YEARS)

#iterate over target years and upload geotiffs as images to earth engine
for i,year in enumerate(TARGET_YEARS):
    #Get index for corresponding year to search through netCDF
    year_index = year-START_YEAR

    #print('Starting year: '+str(year))

    #Format start and end data in miliseconds since the epoch
    start_date = datetime.datetime(year=year,month=1,day=1)
    end_date = datetime.datetime(year=year,month=12,day=31)
    start_date = formatDate(start_date)
    end_date = formatDate(end_date)

    #Get data for variables
    nc_data = np.zeros((len(VARIABLES),ysize,xsize))
    for var_index, var in enumerate(VARIABLES):
        nc_data[var_index] = GetnetCDFDataByName(NC_FILENAME,var,index=year_index).data

    #print('Got data for year: '+str(year))

    #Create geotiff
    create_geotiff(DATA_DIR+'temp_'+str(year),nc_data,NDV, xsize, ysize, GeoT, Projection)

    #print('Created GeoTIFF for year: '+str(year))

    #Upload geotiff to staging bucket
    cmd = ['gsutil','-m','cp',DATA_DIR+'temp_{year}.tif'.format(year=year),GS_BUCKET]
    subprocess.call(cmd)

    #Get asset id and format band names
    asset_id = EE_COLLECTION+'/'+'States_'+str(year)
    bands = ','.join(VARIABLES)

    #Upload tiff from bucket to image on Earth Engine and get Task ID
    cmd = ['earthengine','upload','image','--asset_id='+asset_id,'--force','--nodata_value='+str(NDV),'--time_start='+str(start_date),'--time_end='+str(end_date),'--bands='+bands,GS_BUCKET+'temp_{year}.tif'.format(year=year)]
    shell_output = subprocess.check_output(cmd)
    shell_output = shell_output.decode("utf-8")
    print(shell_output)
    #Get task id
    if 'Started upload task with ID' in shell_output:
        task_id = shell_output.split(': ')[1]
        task_id = task_id.strip()
        #Save task id to task id list
        task_ids[i] = task_id
    else:
        print('Something went wrong!')

    #print('Uploaded asset for year: '+str(year))

    #Remove tiff from my folder
    os.remove(DATA_DIR+'temp_{year}.tif'.format(year=year))

    #If pause for overload is set to true, every NUM_ASSETS_AT_ONCE timesteps, wait for all tasks to finish and remove files from gsutil
    if PAUSE_FOR_OVERLOAD:
        if i% NUM_ASSETS_AT_ONCE == 0:
            #Wait for all tasks to finish
            cmd = ['earthengine','task','wait','all']
            subprocess.call(cmd)
            #Remove tiffs from google cloud bucket
            cmd = ['gsutil','-m','rm',GS_BUCKET+'*']
            subprocess.call(cmd)

for i,year in enumerate(TARGET_YEARS):
    asset_id = EE_COLLECTION+'/'+'States_'+str(year)
    #earthengine task info TASK_ID
    cmd = ['earthengine','task','wait',task_ids[i]]
    subprocess.call(cmd)

    #Set year property of asset
    string = '(number)year='+str(year)
    cmd = ['earthengine', 'asset', 'set', '-p', string, asset_id]
    subprocess.call(cmd)
    
    #Set no data value property of asset
    string = '(number)no_data_value='+str(NDV)
    cmd = ['earthengine', 'asset', 'set', '-p', string, asset_id]
    subprocess.call(cmd)
    
    #Remove tiff from google cloud bucket
    cmd = ['gsutil','rm',GS_BUCKET+'temp_{year}.tif'.format(year=year)]
    subprocess.call(cmd)
