import numpy as np
import pandas as pd
from sqlalchemy import create_engine

def anchor_strategy():
    ############## Read in Data ##################
    #read in postcode to latlong data and pension data
    pcode_LL = pd.read_csv("G:/PerfInfo/Performance Management/PIT Adhocs/2021-2022/Hannah/Maps/pcode_LSOA_latlong.csv",
                                usecols = ['pcds', 'lat', 'long'])
    pension = pd.read_excel('C:/Users/obriene/Projects/Anchor Strategy/Pension Opt Out/Pension Opt Out Summary 16-09-2024.xlsx',
                            usecols=['Banding', 'Staff Group', 'Age Band', 'FTE', 'Pension Opt Out']).rename(columns={'Band ':'Banding', 'Age Band':'AgeBand'})
    #Read in employee data and imd from cl3-data
    cl3_engine = create_engine('mssql+pyodbc://@cl3-data/DataWarehouse?'\
                            'trusted_connection=yes&driver=ODBC+Driver+17'\
                                '+for+SQL+Server')
    #STAFF DATA
    Band_pcds_query = """SELECT *
    FROM [DataWarehouse].[HumanResources].[vw_CurrentStaffPostcodes]
    WHERE Banding LIKE '%Band%' OR Banding LIKE '%Medical%'
    AND PostCode IS NOT NULL
    """
    Band_pcds = pd.read_sql(Band_pcds_query, cl3_engine).rename(columns={'PostCode':'pcds', 'StaffGroup':'Staff Group'})
    #IMD DATA
    imd_query = """SELECT PostcodeVariable as pcds, IndexValue as IMD
    FROM [SDMartDataLive2].[PiMSMarts].[Reference].[vw_IndicesOfMultipleDeprivation2019_DecileByPostcode]
    """
    imd = pd.read_sql(imd_query, cl3_engine)
    cl3_engine.dispose()

    ############## Tidy Data ##################
    #Group up bands
    conds = [Band_pcds['Banding'].isin(['Apprentice/Band 1', 'Band 2', 'Band 3']),
            Band_pcds['Banding'].isin(['Band 4', 'Band 5']),
            Band_pcds['Banding'].isin(['Band 6', 'Band 7']),
            Band_pcds['Banding'].isin(['Band 8A', 'Band 8B', 'Band 8C', 'Band 8D', 'Band 9']),
            Band_pcds['Banding'].isin(['Medical'])]
    groups = ['Bands 1-3', 'Bands 4-5', 'Bands 6-7', 'Bands 8+', 'Medical']
    Band_pcds['Band Groups'] = np.select(conds, groups)
    #Add in band number and days in position
    Band_pcds['Band No'] = Band_pcds['Banding'].str.extract(r'(\d+)')
    Band_pcds['Days in Position'] = (pd.Timestamp.now()
                                     - pd.to_datetime(Band_pcds['StartDateInPosition'])).dt.days
    #Remove missing and add in space and capitalise postcodes, join on imd data
    Band_pcds = Band_pcds.dropna(subset='pcds').copy()
    Band_pcds['pcds'] = [pcd.upper()  if ' ' in pcd
                        else (pcd[:-3]+' '+pcd[-3:]).upper()
                        for pcd in Band_pcds['pcds'].tolist()]
    Band_pcds = Band_pcds.merge(imd, on='pcds', how='left')
    #Group up lat long to postcode minus final 2 charecters for anonymity and merge
    #onto table
    Band_pcds['Area'] = Band_pcds['pcds'].str[:-2]
    pcode_LL['Area'] = pcode_LL['pcds'].str[:-2]
    pcode_LL = pcode_LL.groupby('Area', as_index=False).agg({'lat':'mean', 'long':'mean'})
    Band_pcds = Band_pcds.merge(pcode_LL, on='Area', how='left')
    #Select/add required columns
    employees = Band_pcds[['Banding', 'Band No', 'Band Groups', 'Staff Group',
                           'IMD', 'AgeBand', 'FTE',  'Days in Position',
                           'lat', 'long']].copy()
    #Dataframes of unique values for BI relationships
    banding = pd.DataFrame(Band_pcds['Banding'].drop_duplicates().reset_index(drop=True))
    staff_groups = pd.DataFrame(Band_pcds['Staff Group'].drop_duplicates().reset_index(drop=True))
    age_bands = pd.DataFrame(Band_pcds['AgeBand'].drop_duplicates().reset_index(drop=True))
    
    return employees, pension, banding, staff_groups, age_bands

employees, pension, banding, staff_groups, age_bands = anchor_strategy()
