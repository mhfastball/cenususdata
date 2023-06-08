import pandas as pd
import requests
import us
import numpy as np

class Consensus_Census():

    def __init__(self, groups):
        self.foos = []
        self.dfs = []
        self.groups = groups.split(',')
        self.split_groups = []

    def agg_micro_url(self, qtype, vars, geo, deets=""):
        api = '&key=2589f43bff5f44c4ac00f35fb41c462745f0c07c'
        if deets == "":
            getsum = '?get=' + 'NAME,' + vars
            geog = '&for=' + geo
        else:
            getsum = '?get=' + 'NAME,' + vars
            geog = '&for=' + geo + '&in=' + deets
        if qtype == 'aggregate':
            agg = 'http://api.census.gov/data/2021/acs/acs5'
            url = agg + getsum + geog + api
            print(url)
            return url
        elif qtype == 'micro':
            micro = 'http://api.census.gov/data/2021/acs/acs5/pums'
            url = micro + getsum + geog + api
            print(url)
            return url

    def return_df(self, qtype, vars, geo, deets=None):
        if deets is not None:
            url = self.agg_micro_url(qtype, vars, geo, deets)
        else:
            url = self.agg_micro_url(qtype, vars, geo)
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

    def census_readin(self, census_file):
        cd = pd.read_csv(census_file, index_col=False)
        cd['census_data'] = cd['description'].str.split("!!").apply(lambda x: ''.join(x[-2:]))
        col_dict = dict(zip(cd['code'], cd['census_data']))
        census_var = cd.code.tolist()
        census_query = self.split_list(census_var, 49)
        for x in range(len(census_query)):
            df = self.return_df('aggregate', census_query[x], 'place:*', 'state:*')
            self.dfs.append(df)
        return self.dfs, col_dict

    def df_list_merge(self, df_list):
        df_list_index = df_list[0]
        for x in range(1, len(df_list)):
            df_list_index = df_list_index.merge(df_list[x], on=['NAME', 'state', 'place'], suffixes=('', '_right'))
        return df_list_index

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

    def return_census(self, file):
        df_list, col_dict = self.census_readin(file)
        bigone = self.df_list_merge(df_list)
        bigone['state_abr'] = [us.states.lookup(fips).abbr if us.states.lookup(fips) else fips for fips in bigone.state]
        bigone['state_abr'] = ['DC' if x == '11' else x for x in bigone.state_abr]
        bigone['city'], bigone['state'] = bigone['NAME'].str.split(',', 1).str
        bigone.city = [x.upper() for x in bigone.city]
        bigone.rename(columns=lambda x: col_dict[x] if x in col_dict else x, inplace=True)
        bigone.set_index('NAME', inplace=True)
        self.divide_makeup(bigone)
        for ind, keyw in enumerate(self.groups):
            bigone = bigone.merge(self.split_groups[ind], how='right', left_index=True, right_index=True, suffixes=('_left', ''))
            cols_to_drop = bigone.filter(like='_left').columns
            bigone = bigone.drop(columns=cols_to_drop)
        bigone.reset_index(inplace=True)
        return bigone
