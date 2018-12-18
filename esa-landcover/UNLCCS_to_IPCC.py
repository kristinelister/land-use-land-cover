import rasterio
import numpy as np
import datetime

#The purpose of this code is to convert ESA Land Cover categorized under UN Land Cover Classisfication System to IPCC Land Cover Classification System

#Load in data files
raster_fn = "esa_landcover_tifs/ESACCI-LC-L4-LCCS-Map-300m-P1Y-{year}-v2.0.7.tif"
raster_output_fn = "esa_landcover_tifs/ESA_{year}_ipcc.tif"
for i in np.arange(1992,2016):
    input_name = raster_fn.format(year=str(i))
    output_name = raster_output_fn.format(year=str(i))
    with rasterio.open(input_name) as src:    
        print('Opened year '+str(i))
        # Read as numpy array
        array = src.read()
        print('Read tif for year '+str(i) +' time: '+str(datetime.datetime.now()))
        profile = src.profile
        print('Read profile for year '+str(i) +' time: '+str(datetime.datetime.now()))
        # Reclassify
        #No data = -1
        array[np.where(array == 0)] = -1
        #Agriculture = 1
        array[np.where((array == 10)|(array == 11)|(array == 12)|(array == 20)|(array == 30)|(array == 40))] = 1
        print(str(i)+' 1. Agriculture' +' time: '+str(datetime.datetime.now()))
        #Forest = 2
        array[np.where((array == 50)|(array == 60)|(array == 61)|(array == 62)|(array == 70)|(array == 71)|(array == 72)|(array == 80)|(array == 81)|(array == 82)|(array == 90)|(array == 100)|(array == 160)|(array == 170))] = 2
        print(str(i)+' 2. Forest' +' time: '+str(datetime.datetime.now()))
        #Grassland = 3
        array[np.where((array == 110)|(array == 130))] = 3
        print(str(i)+' 3. Grassland' +' time: '+str(datetime.datetime.now()))
        #Wetland = 4
        array[np.where(array == 180)] = 4
        print(str(i)+' 4. Wetland' +' time: '+str(datetime.datetime.now()))
        #Settlement = 5
        array[np.where(array == 190)] = 5
        print(str(i)+' 5. Settlement' +' time: '+str(datetime.datetime.now()))
        #Shrubland = 6
        array[np.where((array == 120)|(array == 121)|(array == 122))] = 6
        print(str(i)+' 6. Shrubland' +' time: '+str(datetime.datetime.now()))
        #Sparse vegetation = 7
        array[np.where((array == 140)|(array == 150)|(array == 151)|(array == 152)|(array == 153))] = 7
        print(str(i)+' 7. Sparse Vegetation' +' time: '+str(datetime.datetime.now()))
        #Bare area = 8
        array[np.where((array == 200)|(array == 201)|(array == 202))] = 8
        print(str(i)+' 8. Bare Area' +' time: '+str(datetime.datetime.now()))
        
        with rasterio.open(output_name, 'w', **profile) as dst:
            # Write to disk
            dst.write(array)
            print(str(i)+' Saved to new tif')