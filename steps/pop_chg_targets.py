import pandas as pd
from util import Pipeline


def combine_targets(pipeline):
    df = pd.DataFrame()
    for table in pipeline.settings['targets_tables']:
        if 'pop_chg_col' in table:
            # only load targets tables that use population change (Kitsap, Pierce and Snohomish)
            df_table = pipeline.get_table(table['name'])
            
            # add start and horizon year columns
            df_table['start'] = table['pop_chg_start']
            df_table['horizon'] = table['pop_chg_horizon']
            
            # combine into single dataframe
            df = pd.concat([df, df_table], ignore_index=True)
    
    return df[['target_id','pop_chg','start','horizon']]