from ensemble import ensemble_locs
from pull_request import Consensus_Census
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

cc = Consensus_Census('Male,Female,Population,Hispanic')

file = r'C:\projects\data_dump\census_cols.csv'

query = '''
select name, shortname, city, state as state_abr, zipcode
from public.venue
where active = true
and country = 'US'
and sales_hierarchy->3->>'name' in ('PRESTO', 'HORNBECK')
order by state
'''

def fuzzy_match(query, choices):
    return process.extractOne(query, choices, scorer=fuzz.token_sort_ratio)[0]

full_census = cc.return_census(file)

sales_rep = ensemble_locs(query)

#return only specific states to speed lookup
specific_states = sales_rep.state_abr.unique().tolist()
sales_rep.set_index('city', inplace=True)

focused_census = full_census[full_census['state_abr'].isin(specific_states)]
foo = focused_census.set_index('city')#temp dataframe to return city lookups

sales_rep['city'] = [fuzzy_match(city, foo.index) for city in sales_rep.index]
sales_rep.reset_index(drop=True, inplace=True)

join_columns = ['state_abr', 'city']
drop_columns = ['state', 'place']
fin = sales_rep.merge(full_census, how='left', on=join_columns)
fin.drop(columns=drop_columns, inplace=True)