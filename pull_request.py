import pandas as pd
import requests
import us
import numpy as np

class ConsensusCensus:

    def __init__(self, api_key):
        self.foos = []
        self.dfs = []
        self.split_groups = []#still need this?
        self.api = '&key=' + api_key

    def agg_micro_url(self, url_query, vars_query, geo, deets=""):
        if deets == "":
            getsum = '?get=' + 'NAME,' + vars_query
            geog = '&for=' + geo
        else:
            getsum = '?get=' + 'NAME,' + vars_query
            geog = '&for=' + geo + '&in=' + deets
        url = url_query+ getsum + geog + self.api
        print(url)
        return url

    #adjust to take in queries
    def return_df(self, url_query, var_query, geo, deets=None):
        if deets is not None:
            url = self.agg_micro_url(url_query, var_query, geo, deets)
        else:
            url = self.agg_micro_url(url_query, var_query, geo)
        response = requests.get(url)
        if response.status_code == 200:
            json_data = response.json()
            header = json_data[0]
            data = json_data[1:]
            return pd.DataFrame(data, columns=header)
        else:
            print("Error:", response.status_code)

    def split_list(self, lst, limit):
        foo = [lst[i:i + limit] for i in range(0, len(lst), limit)]
        for x in range(len(foo)):
            bar = ','.join(foo[x])
            self.foos.append(bar)
        return self.foos

    def df_list_merge(self, df_list):
        df_list_index = df_list[0]
        for x in range(1, len(df_list)):
            df_list_index = df_list_index.merge(df_list[x], on=['NAME', 'state', 'place'], suffixes=('', '_right'))
        return df_list_index

    def census_dl(self, url_query, cd):#cd short for census dataframe
        #cd = pd.read_csv(census_file, index_col=False)
        cd['census_data'] = cd['label'].str.split("!!").apply(lambda x: ''.join(x[-2:]))
        #translate from census column ID to the label
        col_dict = dict(zip(cd['name'], cd['census_data']))
        census_var = cd['name'].tolist()
        census_query = self.split_list(census_var, 49)
        for x in range(len(census_query)):
            df = self.return_df(url_query, census_query[x], 'place:*', 'state:*')
            self.dfs.append(df)
        df = self.df_list_merge(self.dfs)
        df['state_abr'] = [us.states.lookup(fips).abbr if us.states.lookup(fips) else fips for fips in df.state]
        df['state_abr'] = ['DC' if x == '11' else x for x in df.state_abr]
        df['city'], df['state'] = df['NAME'].str.split(',', 1).str
        df.city = [x.upper() for x in df.city]
        #renames columns using column dict above
        df.rename(columns=lambda y: col_dict[y] if y in col_dict else y, inplace=True)
        return df

    def divide_makeup(self, df):
        for group in self.groups:
            filter_df = df.filter(regex=group)
            filter_df = filter_df.apply(pd.to_numeric, errors='coerce')
            df_columns = filter_df.columns.tolist()
            group_cat_col = [col for col in df_columns if group in col]
            set_denominator = group_cat_col[0]
            the_rest = group_cat_col[1:]
            for cal_col in the_rest:
                makeup_cal_col = cal_col + ' %'
                filter_df[makeup_cal_col] = filter_df[cal_col]/filter_df[set_denominator]
                filter_df = filter_df.reindex(sorted(filter_df.columns, key=lambda x: group in x), axis=1)
            self.split_groups.append(filter_df)

    def return_census(self, url_query, df):
        df = self.census_dl(url_query, df)
        df.set_index('NAME', inplace=True)
        '''
        self.divide_makeup(df) 
        for ind, keyw in enumerate(self.groups):
            df = df.merge(self.split_groups[ind], how='right', left_index=True, right_index=True, suffixes=('_left', ''))
            cols_to_drop = df.filter(like='_left').columns
            df = df.drop(columns=cols_to_drop)
        df.reset_index(inplace=True)
        '''
        return df
