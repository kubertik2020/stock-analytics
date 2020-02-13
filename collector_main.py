import pandas
from datetime import date
from collector import *

today = date.today()
today = today.strftime("%B_%d_%Y")
symbols = pandas.read_csv('resources/symbols.txt', sep='|')

#control how many symbols to collect
symbols = symbols['Symbol'][0:5]

#raw file locations
raw_file_dir_1d = 'data/daily/' + today + '/1d/raw/'
raw_file_dir_1m = 'data/daily/' + today + '/1m/raw/'

#converted to df, location
df_file_1m_converted = 'data/daily/' + today + '/1m/converted/'

#consolidated into a single df
df_file_1d_consolidated = 'data/daily/' + today + '/1d/consolidated.csv'
df_file_1m_consolidated = 'data/daily/' + today + '/1m/consolidated.csv'

#enrched with additional attributes
df_file_1d_enriched = 'data/daily/' + today + '/1d/enriched.csv'



###############################1-day sequence begin###############################
#collect 1day raw data
collector(symbols).collect('1d', 1, raw_file_dir_1d)
#convert to df and consolidate the 1 day data into a single df
df = convert_to_df(raw_file_dir_1d)
df.to_csv(df_file_1d_consolidated)

enrich(df).to_csv(df_file_1d_enriched)
###############################1-day sequence end###############################


###############################1-minute sequence begin###############################
#collect 1m daily data
collector(symbols).collect('1m', 1, raw_file_dir_1m)
#convert the raw data into separate df's
convert(raw_file_dir_1m, df_file_1m_converted)
#consolidate the separate df's into a single df
concat(df_file_1m_converted).to_csv(df_file_1m_consolidated)
###############################1-minute sequence end###############################


