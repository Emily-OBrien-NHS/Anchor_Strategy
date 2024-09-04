import os
import folium
import xlsxwriter
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from sqlalchemy import create_engine
from folium.plugins import HeatMap
from datetime import datetime
os.chdir('C:/Users/obriene/Projects/Anchor Strategy/Outputs')
run_date = datetime.today().strftime('%Y-%m-%d')
pension_data_filename = 'Pension Opt Out Summary 04-09-2024.xlsx'

#Only map/export data for plymouth postcodes
plymouth_only = False
phlebotomy = False

print('reading in data')
#readin postcode to latlong data
pcode_LL = pd.read_csv("G:/PerfInfo/Performance Management/PIT Adhocs/2021-2022/Hannah/Maps/pcode_LSOA_latlong.csv",
                            usecols = ['pcds', 'lat', 'long'])

#Read in employee data
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
imd_query = """SELECT PostcodeVariable as pcds, IndexValue
FROM [SDMartDataLive2].[PiMSMarts].[Reference].[vw_IndicesOfMultipleDeprivation2019_DecileByPostcode]
"""
imd = pd.read_sql(imd_query, cl3_engine)

cl3_engine.dispose()

#Group up bands
conds = [Band_pcds['Banding'].isin(['Apprentice/Band 1', 'Band 2', 'Band 3']),
         Band_pcds['Banding'].isin(['Band 4', 'Band 5']),
         Band_pcds['Banding'].isin(['Band 6', 'Band 7']),
         Band_pcds['Banding'].isin(['Band 8A', 'Band 8B', 'Band 8C', 'Band 8D', 'Band 9']),
         Band_pcds['Banding'].isin(['Medical'])]
groups = ['Bands 1-3', 'Bands 4-5', 'Bands 6-7', 'Bands 8+', 'Medical']
Band_pcds['Band Groups'] = np.select(conds, groups)

#Add in band number
Band_pcds['Band No'] = Band_pcds['Banding'].str.extract(r'(\d+)')

#Remove missing and add in space and capitalise postcodes, join on imd data
Band_pcds = Band_pcds.dropna(subset='pcds').copy()
Band_pcds['pcds'] = [pcd.upper()  if ' ' in pcd
                     else (pcd[:-3]+' '+pcd[-3:]).upper()
                     for pcd in Band_pcds['pcds'].tolist()]
Band_pcds = Band_pcds.merge(imd, on='pcds', how='left')
Band_pcds['FTE'] = Band_pcds['FTE'].astype(float)
total_fte, total_headcount = Band_pcds.agg({'FTE':'sum', 'Band Groups':'count'})

#Filter to only plymouth postcodes if true
if plymouth_only:
     Band_pcds = Band_pcds.loc[Band_pcds['pcds'].str.contains(r'PL[0-9] ')].copy()
#filter to only phlebotomists if true
if phlebotomy:
     Band_pcds = Band_pcds.loc[Band_pcds['PositionTitle'].str.contains('Phlebotomist')].copy()

#Merge data together, group up data to postcode up to last 2 charecters to keep annonymity
LL_df = pcode_LL.merge(Band_pcds, on='pcds', how='inner')
LL_df['Area'] = LL_df['pcds'].str[:-2]
LL_df = LL_df.groupby(['Band Groups', 'Area'], as_index=False).agg({'lat':'mean', 'long':'mean', 'Banding':'count'})


# =============================================================================
# #HEATMAP
# =============================================================================
print('creating maps')
for group in LL_df['Band Groups'].drop_duplicates().tolist():
     m = folium.Map(location = [50.4163,-4.11849],
                    zoom_start=10,
                    min_zoom = 7,
                    tiles='cartodbpositron')
     heat_df = LL_df.loc[LL_df['Band Groups'] == group].copy()
     heat_df = heat_df.loc[heat_df.index.repeat(heat_df['Banding']), ['lat','long']].dropna()#Repeat by number of patients in each pcode
     #Make a list of values
     heat_data = [[row['lat'],row['long']] for index, row in heat_df.iterrows()]
     HeatMap(heat_data).add_to(m)
     m.save('G:/FBM/Operational Research/Employee Banding/Maps/' + run_date + ' ' + group + ' heatmap.html')

# =============================================================================
# #Table Data
# =============================================================================
#read in pension data
pension = pd.read_excel(f'C:/Users/obriene/Projects/Anchor Strategy/Pension Opt Out/{pension_data_filename}')
pension = pension.rename(columns={'Band ':'Banding', 'Age Band':'AgeBand'})

#Functions
def parse_date(td):
    #Conerts difference between two dates into string of xY xm
    resYear = ((td.dt.days)/364.0).astype(float)                 # get the number of years including the the numbers after the dot
    resMonth = ((resYear - resYear.astype(int))*364/30).astype(int).astype(str)  # get the number of months, by multiply the number after the dot by 364 and divide by 30.
    resYear = (resYear).astype(int).astype(str)
    return resYear + "Y " + resMonth + "m"

def group_data(df, pen, cols):
     #Function to group up data by different values and produce the results.
     df = df.groupby(cols).agg({'Banding':'count', 'FTE':'sum', 'Days in Position':['mean', 'max']}).reset_index()
     df.columns = cols + ['Headcount', 'FTE', 'Average Time in Position', 'Max Time in Position']
     df['Average Time in Position'] = parse_date(df['Average Time in Position'])
     df['Max Time in Position'] = parse_date(df['Max Time in Position'])
     #Add pension data
     df['Proportion of Total Headcount'] = (df['Headcount'] / total_headcount)
     df['Proportion of Total FTE'] = (df['FTE'] / total_fte)
     df = df.merge(pen.groupby(cols, as_index=False)['Pension Opt Out'].count(), on=cols, how='left')
     return df

def all_data(df, pen):
     all = pd.DataFrame({'Headcount':[df['Banding'].count()], 'FTE':[df['FTE'].sum()],
                   'Average Time in Position':[df['Days in Position'].mean()],
                   'Max Time in Position':[df['Days in Position'].max()]})
     all['Average Time in Position'] = parse_date(all['Average Time in Position'])
     all['Max Time in Position'] = parse_date(all['Max Time in Position'])
     #Add pension data
     all['Proportion of Total Headcount'] = (all['Headcount'] / total_headcount)
     all['Proportion of Total FTE'] = (all['FTE'] / total_fte)
     all['Pension Opt Out'] = pen['Pension Opt Out'].count()
     all['Staff Group'] = 'All'
     all['Banding'] = 'All'
     return all


#Select/add required columns
df = Band_pcds[['Banding', 'Band No', 'Band Groups', 'Staff Group', 'FTE', 'AgeBand', 'StartDateInPosition']].copy()
df['Days in Position'] = (pd.Timestamp.now() - pd.to_datetime(df['StartDateInPosition']))

#Get the grouped tables required
band_name = group_data(df, pension, ['Banding'])
staff_group = group_data(df, pension, ['Staff Group'])
band_and_staff = group_data(df, pension, ['Banding', 'Staff Group'])
all = all_data(df, pension)
band_name['Staff Group'] = 'All'
staff_group['Banding'] = 'All'
agg_data = pd.concat([band_and_staff, band_name, staff_group, all])
lookup_col = agg_data[['Banding', 'Staff Group']].astype(str).agg(' '.join, axis=1)
agg_data.insert(loc=0, column='lookup', value=lookup_col)

band_age_band = group_data(df, pension, ['Banding', 'AgeBand'])
staff_age_band = group_data(df, pension, ['Staff Group', 'AgeBand'])
band_age_band['Staff Group'] = 'All'
staff_age_band['Banding'] = 'All'
band_and_staff_age_band = group_data(df, pension, ['Banding', 'Staff Group', 'AgeBand'])
all_age_band = group_data(df, pension, ['AgeBand'])
all_age_band['Staff Group'] = 'All'
all_age_band['Banding'] = 'All'
agg_age_data = pd.concat([band_and_staff_age_band, band_age_band, staff_age_band, all_age_band])
lookup_col = agg_age_data[['Banding', 'Staff Group', 'AgeBand']].astype(str).agg(' '.join, axis=1)
agg_age_data.insert(loc=0, column='lookup', value=lookup_col)


##############To excel ##################
print('creating excel output')
#Lists for formatting
staff_groups = agg_data['Staff Group'].drop_duplicates().tolist()
bands = agg_data['Banding'].drop_duplicates().tolist()
ages = ['<=20 Years', '21-25', '26-30', '31-35', '36-40', '41-45', '46-50', '51-55', '56-60', '61-65', '66-70', '>=71 Years']
cols = ['B', 'C', 'D', 'E', 'F', 'G', 'H']
col_names = band_name.columns[1:].tolist()

#Excel Writer
writer = pd.ExcelWriter(f"G:/FBM/Operational Research/Employee Banding/{run_date}_aggregate_employee_band_data.xlsx", engine='xlsxwriter')
workbook = writer.book

####COVER SHEET####
#Add formats
white = workbook.add_format({'bg_color':'white'})
yellow = workbook.add_format({'align':'center', 'border':True, 'bg_color':'yellow'})
center = workbook.add_format({'align':'center'})
center_border = workbook.add_format({'align':'center', 'border':True})
bold_right = workbook.add_format({'bold':True, 'align':'right', 'border':2})
bold_wrap = workbook.add_format({'bold': True, 'align':'center', 'valign':'center', 'text_wrap':True, 'border':2})
percent_format = workbook.add_format({"num_format": "0%", 'align':'center', 'border':True})
percent_format2 = workbook.add_format({"num_format": "0%"})

#Add filter worksheet
worksheet = workbook.add_worksheet('Filter')

#Set general column formats
worksheet.set_column(0, 27, 8, white)
worksheet.set_column(0, 0, 26, white)
worksheet.set_column(1, 7, 10, white)

#Add band and staff group drop down cells
worksheet.write('A1', 'Staff Group:', bold_right)
worksheet.write('B1', '', yellow)
worksheet.data_validation('B1', {'validate':'list',
                                 'source':staff_groups})
worksheet.write('A2', 'Band:', bold_right)
worksheet.write('B2', '', yellow)
worksheet.data_validation('B2', {'validate':'list',
                                 'source':bands})

#Add in text for band level lookups
for i, age in enumerate(ages):
     worksheet.write(f'A{i+13}', age, bold_right)
#Add band level vlookups
worksheet.write_formula('B4', '''=IFERROR(VLOOKUP(B2&" "&B1,'Agg Data'!A:J,4,0), 0)''', center_border)
worksheet.write_formula('B5', '''=IFERROR(VLOOKUP(B2&" "&B1,'Agg Data'!A:J,5,0), 0)''', center_border)
worksheet.write_formula('B6', '''=IFERROR(VLOOKUP(B2&" "&B1,'Agg Data'!A:J,6,0), "-")''', center_border)
worksheet.write_formula('B7', '''=IFERROR(VLOOKUP(B2&" "&B1,'Agg Data'!A:J,7,0), "-")''', center_border)
worksheet.write_formula('B8', '''=IFERROR(VLOOKUP(B2&" "&B1,'Agg Data'!A:J,8,0), "-")''', percent_format)
worksheet.write_formula('B9', '''=IFERROR(VLOOKUP(B2&" "&B1,'Agg Data'!A:J,9,0), "-")''', percent_format)
worksheet.write_formula('B10', '''=IFERROR(VLOOKUP(B2&" "&B1,'Agg Data'!A:J,10,0), "-")''', center_border)

#Add text for age band level lookups
for i, col in enumerate(zip(cols, col_names)):
     worksheet.write(f'A{i+4}', col[1], bold_right)
     worksheet.write(f'{col[0]}12', col[1], bold_wrap)
#Add age band lookups
for i in range(13,25):
     #Headcount
     worksheet.write_formula(f'B{i}',f'''=IFERROR(VLOOKUP((B2&" "&B1&" "&A{i}),'Agg Age Band Data'!A:K,5,0), 0)''', center_border)
     #FTE
     worksheet.write_formula(f'C{i}', f'''=IFERROR(VLOOKUP((B2&" "&B1&" "&A{i}),'Agg Age Band Data'!A:K,6,0), 0)''', center_border)
     #Average Time in Position
     worksheet.write_formula(f'D{i}', f'''=IFERROR(VLOOKUP((B2&" "&B1&" "&A{i}),'Agg Age Band Data'!A:K,7,0), "-")''', center_border)
     #Longest Time in Position
     worksheet.write_formula(f'E{i}', f'''=IFERROR(VLOOKUP((B2&" "&B1&" "&A{i}),'Agg Age Band Data'!A:K,8,0), "-")''', center_border)
     #Proportion of Total Headcount
     worksheet.write_formula(f'F{i}', f'''=IFERROR(VLOOKUP((B2&" "&B1&" "&A{i}),'Agg Age Band Data'!A:K,9,0), "-")''', percent_format)
     #Proportion of FTE
     worksheet.write_formula(f'G{i}', f'''=IFERROR(VLOOKUP((B2&" "&B1&" "&A{i}),'Agg Age Band Data'!A:K,10,0), "-")''', percent_format)
     #Pension Opt Out
     worksheet.write_formula(f'H{i}', f'''=IFERROR(VLOOKUP((B2&" "&B1&" "&A{i}),'Agg Age Band Data'!A:K,11,0), "-")''', center_border)

#Add in bar charts
headcount_chart = workbook.add_chart({'type':'column'})
headcount_chart.add_series({'name':'Headcount',
                            'categories': 'Filter!$A$13:$A$24',
                            'values': 'Filter!$B$13:$B$24'})
headcount_chart.add_series({'name':'FTE',
                            'categories': 'Filter!$A$13:$A$24',
                            'values': 'Filter!$C$13:$C$24'})
headcount_chart.set_title({'name':'Headcount and FTE'})
headcount_chart.set_x_axis({'name':'Age Bands', 'num_font':{'rotation':45}})
headcount_chart.set_style(2)
worksheet.insert_chart('J2', headcount_chart, {'x_scale':1.04, 'y_scale':1.07})

pension_chart = workbook.add_chart({'type':'column'})
pension_chart.add_series({'name':'Pension Opt Out',
                            'categories': 'Filter!$A$13:$A$24',
                            'values': 'Filter!$H$13:$H$24'})
pension_chart.set_title({'name':'Pension Opt Out'})
pension_chart.set_x_axis({'name':'Age Bands', 'num_font':{'rotation':45}})
pension_chart.set_style(2)
pension_chart.set_legend({'none':True})
worksheet.insert_chart('J17', pension_chart, {'x_scale':1.04, 'y_scale':1.07})

####Add lookup sheets####
agg_data.to_excel(writer, sheet_name='Agg Data', index=False, engine='xlsxwriter')
band_worksheet = writer.sheets['Agg Data']
band_worksheet.set_column(0, 2, 16)
band_worksheet.set_column(3, 8, 11, center)
band_worksheet.set_column(6, 7, 16, percent_format2)

agg_age_data.to_excel(writer, sheet_name='Agg Age Band Data', index=False, engine='xlsxwriter')
age_worksheet = writer.sheets['Agg Age Band Data']
age_worksheet.set_column(0, 3, 26)
age_worksheet.set_column(4, 12, 11, center)
age_worksheet.set_column(9, 10, 16, percent_format2)

####save and close the workbook####
writer.close()
