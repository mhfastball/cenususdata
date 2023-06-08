from ensemble import ensemble_locs
from pull_request import Consensus_Census
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def fuzzy_match(query, choices):
    return process.extractOne(query, choices, scorer=fuzz.token_sort_ratio)[0]

def main(file, query):
    cc = Consensus_Census('Male,Female,Population,Hispanic')

    full_census = cc.return_census(file)
    sales_rep = ensemble_locs(query)

    #return only specific states to speed lookup
    specific_states = sales_rep.state_abr.unique().tolist()
    sales_rep.set_index('city', inplace=True)

    focused_census = full_census[full_census['state_abr'].isin(specific_states)]
    foo = focused_census.set_index('city')#temp dataframe to return city lookups

    sales_rep['city'] = [fuzzy_match(city, foo.index) for city in sales_rep.index]
    sales_rep.reset_index(drop=True, inplace=True)

    fin = sales_rep.merge(full_census, how='left', on=['state_abr', 'city'])
    fin.drop(columns=['state', 'place'], inplace=True)

    return fin
