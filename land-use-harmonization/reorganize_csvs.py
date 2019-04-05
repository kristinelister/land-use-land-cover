import pandas as pd
import os
import numpy as np

folder_conv = '/Users/kristine/WRI/NationalGeographic/Phase1/LUH_historical_baseline_{level}_area'
fname_conv = 'luh_baseline_{level}_area_{year}.csv'
out_fname_conv = 'luh_baseline_{level}_{crop}_{type}.csv'
out_folder_conv = ''


levels = ['biome','ecoregion']
categories = ['cropland','pasture','agriculture','forest']
years = np.arange(1700,2020,10)


#years = np.arange(1800,1801)




for level in levels:
    os.chdir(folder_conv.format(level=level))
    fname = fname_conv.format(level=level, year=years[0])
    df_format = pd.read_csv(fname,header=0)
    if level == 'ecoregion':
        columns = df_format['ECO_NAME'].values
    elif level == 'biome':
        columns = df_format['Biome'].values
    df_crop_area = pd.DataFrame(columns=columns)
    df_pasture_area = pd.DataFrame(columns=columns)
    df_agriculture_area = pd.DataFrame(columns=columns)
    df_forest_area = pd.DataFrame(columns=columns)
    df_crop_percent = pd.DataFrame(columns=columns)
    df_pasture_percent = pd.DataFrame(columns=columns)
    df_agriculture_percent = pd.DataFrame(columns=columns)
    df_forest_percent = pd.DataFrame(columns=columns)
    for year in years:
        fname = fname_conv.format(level=level, year=year)
        df = pd.read_csv(fname,header=0)
        cropland = df['Cropland'].values
        pasture = df['Rangeland'].values + df['Pastureland'].values
        forest = df['Forest'].values
        agriculture = cropland + pasture
        total = df['Total'].values
        cropland_percent = np.divide(cropland,total, out=np.zeros_like(cropland), where=total!=0)
        pasture_percent = np.divide(pasture,total, out=np.zeros_like(pasture), where=total!=0)
        agriculture_percent = np.divide(agriculture,total, out=np.zeros_like(agriculture), where=total!=0)
        forest_percent = np.divide(forest,total, out=np.zeros_like(forest), where=total!=0)
        print(len(df_crop_area))
        if len(df_crop_area) == 0:
            next_index = 0
        else:
            next_index = df_crop_area.index[-1] + 1
        df_crop_area.at[next_index] = cropland
        df_pasture_area.at[next_index] = pasture
        df_agriculture_area.at[next_index] = agriculture
        df_forest_area.at[next_index] = forest
        df_crop_percent.at[next_index] = cropland_percent
        df_pasture_percent.at[next_index] = pasture_percent
        df_agriculture_percent.at[next_index] = agriculture_percent
        df_forest_percent.at[next_index] = forest_percent
    df_crop_area['Year'] = years
    df_pasture_area['Year'] = years
    df_agriculture_area['Year'] = years
    df_forest_area['Year'] = years
    df_crop_percent['Year'] = years
    df_pasture_percent['Year'] = years
    df_agriculture_percent['Year'] = years
    df_forest_percent['Year'] = years
    
    cols = df_crop_area.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df_crop_area = df_crop_area[cols]
    df_pasture_area = df_pasture_area[cols]
    df_agriculture_area = df_agriculture_area[cols]
    df_forest_area = df_forest_area[cols]
    df_crop_percent = df_crop_percent[cols]
    df_pasture_percent = df_pasture_percent[cols]  
    df_agriculture_percent = df_agriculture_percent[cols]
    df_forest_percent = df_forest_percent[cols]   
    
    df_crop_area.to_csv(out_fname_conv.format(level=level,crop='crop',type='area'),encoding='utf-8',index=False)
    df_pasture_area.to_csv(out_fname_conv.format(level=level,crop='pasture',type='area'),encoding='utf-8',index=False)
    df_agriculture_area.to_csv(out_fname_conv.format(level=level,crop='agriculture',type='area'),encoding='utf-8',index=False)
    df_forest_area.to_csv(out_fname_conv.format(level=level,crop='forest',type='area'),encoding='utf-8',index=False)
    df_crop_percent.to_csv(out_fname_conv.format(level=level,crop='crop',type='percent'),encoding='utf-8',index=False)
    df_pasture_percent.to_csv(out_fname_conv.format(level=level,crop='pasture',type='percent'),encoding='utf-8',index=False)
    df_agriculture_percent.to_csv(out_fname_conv.format(level=level,crop='agriculture',type='percent'),encoding='utf-8',index=False)
    df_forest_percent.to_csv(out_fname_conv.format(level=level,crop='forest',type='percent'),encoding='utf-8',index=False)
