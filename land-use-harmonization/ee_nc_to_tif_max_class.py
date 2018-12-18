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

#The purpose of this code is to upload data from the Land-Use Harmonization version 2 to google earth engine
#These rasters are categorized by the majority land cover class in that pixel
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

EE_FOLDER = 'users/resourcewatchlandcover/LUH_Classified'
EE_COLLECTION = EE_FOLDER+ '/LUH_Max_REMIND'
GS_FOLDER = 'LandUseHarmonization'
GS_BUCKET = 'gs://upload_bucket/'

DATA_DIR = 'LUH_data/data/'
NC_FILENAME = DATA_DIR+'REMIND_states.nc'
START_YEAR = 2015

VARIABLES = ['urban','c3ann','c4ann','c3per','c4per','c3nfx','range','pastr','primf','secdf','primn','secdn']
NEW_VARIABLES = ['urban','crops','range','pastr','forest','nonforest']
META_VARIABLE = 'primf'

PAUSE_FOR_OVERLOAD = True
NUM_ASSETS_AT_ONCE = 50

ASSET_ID = EE_COLLECTION+'/'+'Classes_REMIND_{year}'

#np.arange(850,2020,5)
TARGET_YEARS = np.arange(2015,2101)


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
    #create a driver
    driver = gdal.GetDriverByName('GTiff')
    DataSet = driver.Create(NewFileName, xsize, ysize, 1,DataType)
    DataSet.SetGeoTransform(GeoT)
    DataSet.SetProjection(Projection.ExportToWkt())
    
    #Write data to geotiff
    DataSet.GetRasterBand(1).WriteArray(Array)
    DataSet.GetRasterBand(1).SetNoDataValue(NDV)
    DataSet.FlushCache()
    return NewFileName
    
    
def formatDate(date):
    '''Format date as ms since last epoch'''
    if isinstance(date, int):
        return date
    seconds = (date - datetime.datetime.utcfromtimestamp(0)).total_seconds()
    return int(seconds * 1000)
    

#Get metadata properties of the netcdf variables
NDV, xsize, ysize, GeoT, Projection = GetnetCDFInfobyName(NC_FILENAME,META_VARIABLE)
    
#Create empty list to save task ID's
task_ids = ['']*len(TARGET_YEARS)
overall_match_count = 0
# #iterate over target years and upload geotiffs as images to earth engine
for i,year in enumerate(TARGET_YEARS):
    #Get index for corresponding year to search through netCDF
    year_index = year-START_YEAR

    #print('Starting year: '+str(year))

    #Format start and end data in miliseconds since the epoch
    start_date = datetime.datetime(year=year,month=1,day=1)
    end_date = datetime.datetime(year=year,month=12,day=31)
    start_date = formatDate(start_date)
    end_date = formatDate(end_date)

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
    
    #Set no data value to -1 to find max class
    nc_data[nc_data>=NDV] = -1
    #Find which variable has the highest percent coverage of each of the variables
    #Add 1 so that class values start at 1 instead of 0... class values are now 1,2,3,4,5,6
    nc_data_max = np.argmax(nc_data,axis=0)+1
    #Using numpy argmax, returns the index along the axis that has the highest value
    #Argmax will return the first index if all values are the same
    #Find where array has the same values across axis 0 (across the variables)
    count=0
    for k in np.arange(ysize):
        for l in np.arange(xsize):
            sorted_arr = np.sort(nc_data[:,k,l])
            if sorted_arr[-1]==sorted_arr[-2] and sorted_arr[-1]!=-1 and sorted_arr[-1]!=0:
                count=count+1
                print(sorted_arr)
    match_index = np.where((nc_data[0,:,:]==nc_data[1,:,:])& (nc_data[0,:,:]==nc_data[2,:,:]) & (nc_data[0,:,:]==nc_data[3,:,:]) & (nc_data[0,:,:] == nc_data[4,:,:])&\
        (nc_data[0,:,:]==nc_data[5,:,:]) & (nc_data[1,:,:] == nc_data[2,:,:]) & (nc_data[1,:,:] == nc_data[3,:,:]) & (nc_data[1,:,:] == nc_data[4,:,:])&\
        (nc_data[1,:,:]==nc_data[5,:,:]) & (nc_data[2,:,:] == nc_data[3,:,:]) & (nc_data[2,:,:] == nc_data[4,:,:]) & (nc_data[2,:,:] == nc_data[5,:,:])&\
        (nc_data[3,:,:]==nc_data[4,:,:]) & (nc_data[3,:,:] == nc_data[5,:,:]) & (nc_data[4,:,:] == nc_data[5,:,:]),True,False)
    #Create new class for when they're all equal, class value 7
    nc_data_max[match_index] == 7
    print(count)
    overall_match_count = count+overall_match_count
    #Find where all classes have 0 coverage
    zero_index = np.where(nc_data[:,:,:]==0,True,False)
    zero_index = np.all(zero_index,axis=0)
    #Set those pixels to 0
    nc_data_max[zero_index] = 0
    
    #Convert to float
    nc_data_max = nc_data_max.astype(float)
    
    #Find where all classes have no data value
    ndv_index = np.where(nc_data[:,:,:]==-1,True,False)
    ndv_index = np.all(ndv_index,axis=0)
    #Set those pixels to no data value (NDV)
    nc_data_max[ndv_index] = NDV
    
    #Create geotiff
    create_geotiff(DATA_DIR+'temp_'+str(year),nc_data_max,NDV, xsize, ysize, GeoT, Projection)

    #print('Created GeoTIFF for year: '+str(year))

    #Upload geotiff to staging bucket
    cmd = ['gsutil','-m','cp',DATA_DIR+'temp_{year}.tif'.format(year=year),GS_BUCKET]
    subprocess.call(cmd)

    #Get asset id and format band names
    asset_id = ASSET_ID.format(year=year)
    bands = ','.join(VARIABLES)

    #Upload tiff from bucket to image on Earth Engine and get Task ID
    cmd = ['earthengine','upload','image','--asset_id='+asset_id,'--force','--time_start='+str(start_date),'--time_end='+str(end_date),'--pyramiding_policy=sample',GS_BUCKET+'temp_{year}.tif'.format(year=year)]
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
        if i% NUM_ASSETS_AT_ONCE == 0 and i!=0:
            #Wait for all tasks to finish
            cmd = ['earthengine','task','wait','all']
            subprocess.call(cmd)
            #Remove tiffs from google cloud bucket
            cmd = ['gsutil','-m','rm',GS_BUCKET+'*']
            subprocess.call(cmd)

for i,year in enumerate(TARGET_YEARS):
    asset_id = ASSET_ID.format(year=year)
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
