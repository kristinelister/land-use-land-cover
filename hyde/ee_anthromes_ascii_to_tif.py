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
import urllib.request
import zipfile

#The purpose of this code is to upload data from HYDE 3.2 land use categorized by anthromes to google earth engine
#Ascii grids are downloaded, converted to geotiffs, and uploaded to google earth engine
#Currently the code is set to upload the BC section of years


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


EE_FOLDER = 'users/resourcewatchlandcover/HYDE/'
EE_COLLECTION = EE_FOLDER+ 'Anthromes'
GS_FOLDER = 'HYDE'
GS_BUCKET = 'gs://upload_bucket/'

DATA_DIR = 'LUH_data/data/'
ASC_FILENAME = DATA_DIR+'{yearstr}_anthromes.asc'


BC_YEARS = np.arange(1000,11000,1000)
AD_YEARS = np.concatenate([np.arange(0,1800,100),np.arange(1710,2010,10),np.arange(2001,2018,1)])

URL = 'ftp://ftp.pbl.nl/hyde/hyde3.2/anthromes/zip/{filename}'
DOWNLOAD_NAME = '{year}{AD}_anthromes.zip'
FILE_NAME = 'anthromes{year}{AD}.asc'
TEMP_FILE_NAME = 'temp_anthromes_{year}{AD}.tif'

PAUSE_FOR_OVERLOAD = True
NUM_ASSETS_AT_ONCE = 50

#HERE IS WHERE YOU SET AD VS BC
#Code is set right now for BC years, this is changed by replacing YEARS=BC_YEARS to YEARS=AD_YEARS below
#       And by replacing AD='BC' below to AD='AD'
AD = 'BC'
YEARS= BC_YEARS


def start_task(year,AD):
    '''
    Function to download datafile, convert to geoTIFF, and upload to earth engine
    '''
    download_name = DOWNLOAD_NAME.format(year=year,AD=AD)
    try:
        urllib.request.urlretrieve(URL.format(filename=download_name), DATA_DIR+download_name)
    except:
        urllib.request.urlretrieve(URL.format(filename=download_name), DATA_DIR+download_name,timeout=600)
    #Extract file from zipped file
    zip_file = zipfile.ZipFile(DATA_DIR+download_name)
    zip_file.extractall(DATA_DIR)
    os.remove(DATA_DIR+download_name)
    
    #Read in asc information, create geotiff driver, and copy over information
    drv = gdal.GetDriverByName('GTiff')
    ds_in = gdal.Open(DATA_DIR+FILE_NAME.format(year=year,AD=AD))
    ds_out = drv.CreateCopy(DATA_DIR+TEMP_FILE_NAME.format(year=year,AD=AD), ds_in)
    
    #Get no data value
    NDV = ds_in.GetRasterBand(1).GetNoDataValue()
    
    #Get projection information and save to new file
    Projection = osr.SpatialReference()
    #Projection.ImportFromWkt(ds_in.GetProjectionRef())
    Projection.ImportFromEPSG(4326)
    ds_out.SetProjection(Projection.ExportToWkt())
    
    #Close files
    ds_in = None
    ds_out = None
    
    #Remove ascii files
    os.remove(DATA_DIR+FILE_NAME.format(year=year,AD=AD))
    
    #Upload geotiff to staging bucket
    cmd = ['gsutil','-m','cp',DATA_DIR+TEMP_FILE_NAME.format(year=year,AD=AD),GS_BUCKET]
    subprocess.call(cmd)
    
    #Remove local file as well
    os.remove(DATA_DIR+TEMP_FILE_NAME.format(year=year,AD=AD))
    
    #Get asset id 
    asset_id = EE_COLLECTION+'/'+'Anthromes_'+str(year)+AD
    
    #Upload GeoTIFF from google storage bucket to earth engine
    cmd = ['earthengine','upload','image','--asset_id='+asset_id,'--force','--nodata_value='+str(NDV),'--pyramiding_policy=sample',GS_BUCKET+TEMP_FILE_NAME.format(year=year,AD=AD)]
    shell_output = subprocess.check_output(cmd)
    shell_output = shell_output.decode("utf-8")
    print(shell_output)
    
    #Get task id
    task_id = ''
    if 'Started upload task with ID' in shell_output:
        task_id = shell_output.split(': ')[1]
        task_id = task_id.strip()
    else:
        print('Something went wrong!')
        task_id='ERROR'
    return task_id,NDV

def add_properties(task_id,year,AD,NDV,DATA_DIR=DATA_DIR,TEMP_FILE_NAME=TEMP_FILE_NAME):
    '''
    Function to add properties to images
    '''
    #Get asset id
    asset_id = EE_COLLECTION+'/'+'Anthromes_'+str(year)+AD
    
    #Wait for task to finish
    cmd = ['earthengine','task','wait',task_id]
    subprocess.call(cmd)
    
    #Set year property of asset
    string=''
    if AD=='AD':
        string = '(number)year='+str(year)
    if AD=='BC':
        string = '(number)year=-'+str(year)
    cmd = ['earthengine', 'asset', 'set', '-p', string, asset_id]
    subprocess.call(cmd)
    
    #Set no data value property of asset
    string = '(number)no_data_value='+str(NDV)
    cmd = ['earthengine', 'asset', 'set', '-p', string, asset_id]
    subprocess.call(cmd)
    
    #Remove tiff from google cloud bucket
    cmd = ['gsutil','rm',GS_BUCKET+TEMP_FILE_NAME.format(year=year,AD=AD)]
    subprocess.call(cmd)
    return task_id

#Initialize NDV variable, it is always set to -9999.0 but I read it in from the ascii grid file later anyway
NDV= -9999.0
#Create empty array for task id's
task_ids = ['']*len(YEARS)

#For each year
for i,year in enumerate(YEARS):
    #Start upload task
    task_ids[i],NDV = start_task(year,AD)
    #If pause for overload is set to true, every NUM_ASSETS_AT_ONCE timesteps, wait for all tasks to finish and remove files from gsutil
    if PAUSE_FOR_OVERLOAD:
        if i% NUM_ASSETS_AT_ONCE == 0:
            #Wait for all tasks to finish
            cmd = ['earthengine','task','wait','all']
            subprocess.call(cmd)
            #Remove tiffs from google cloud bucket
            cmd = ['gsutil','-m','rm',GS_BUCKET+'*']
            subprocess.call(cmd)

#Add properties
for i,year in enumerate(YEARS):
    add_properties(task_ids[i],year,AD,NDV)
