import os
from util import Pipeline, CensusApi


def get_dec_block_data(pipeline):
    p = pipeline
    api_key = os.getenv(p.settings['CensusKey'])
    c = CensusApi(api_key)
    census_year = p.settings.get('census_year')

    county_ids = p.settings['county_ids']
    state_id = p.settings['state_id']
    dec_cols_dict = p.settings['census_variables']

    dec = (
    c.get_dec_data(dec_cols_dict, census_year, 'block', 'pl', county_ids,state_id)
    .drop(columns='name')
    )

    p.save_table('dec_block_data', dec)

def run_step(context):
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print("Getting Decennial Census block data and saving to HDF5...")
    get_dec_block_data(p)
    return context