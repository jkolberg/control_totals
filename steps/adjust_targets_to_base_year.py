import pandas as pd
from util import Pipeline

def combine_targets(pipeline, target_type):
    # target_type: 'total_pop' or 'units'
    df = pd.DataFrame()
    for table in pipeline.settings['targets_tables']:
        if f'{target_type}_chg_col' in table:
            df_table = pipeline.get_table(table['name'])

            # add start year column
            df_table['start'] = table[f'{target_type}_chg_start']

            df = pd.concat([df, df_table], ignore_index=True)
    
    return df[['target_id', f'{target_type}_chg', 'start']]


def sum_ofm_to_target_area(pipeline, year, target_type):
    # target_type: 'total_pop' or 'units'

    p = pipeline
    
    # get control to target lookup
    control_target_lookup = p.get_table('control_target_lookup')
    
    # sum ofm estimates by control area
    ofm = (
        p.get_table(f'ofm_estimates_{year}_by_control_area')
        # add year suffix to ofm column
        .rename(columns={f'ofm_{target_type}':f'ofm_{target_type}_{year}'})
        # join to target ids
        .merge(control_target_lookup[['control_id', 'target_id']], on='control_id', how='left')
        # groupby sum to target id
        .groupby('target_id').sum().reset_index()
        # return only target id and needed ofm column
        [['target_id', f'ofm_{target_type}_{year}']]
    )
    return ofm

def get_ofm_all_years(pipeline, start_years, target_type):
    p = pipeline
    base_year = p.settings['base_year']

    # create empty dataframe to hold all years of needed ofm columns
    ofm_all_years = pd.DataFrame()
    
    # loop through baseyear and start years and sum ofm to target area

    for start_year in list(set([base_year] + start_years)):
        ofm_df = sum_ofm_to_target_area(p, start_year, target_type)

        # merge to all years dataframe
        ofm_all_years = ofm_all_years.merge(ofm_df, on='target_id', how='outer') if not ofm_all_years.empty else ofm_df

    return ofm_all_years


def adjust_targets(pipeline, target_type):
    # target_type: 'pop' or 'units'

    p = pipeline
    base_year = p.settings['base_year']

    # combine county targets
    df = combine_targets(p, target_type)

    # get unique start years in the targets
    start_years = df['start'].unique().tolist()

    # get ofm for all start years and base year amd merge to targets
    ofm_all_years = get_ofm_all_years(p, start_years, target_type)
    df = df.merge(ofm_all_years, on='target_id', how='left')

    # loop through each row to calculate ofm change from target start year to base year
    for index, row in df.iterrows():
        start = int(row['start'])
        ofm_start_col = f'ofm_{target_type}_{start}'
        ofm_base_col = f'ofm_{target_type}_{base_year}'
        ofm_chg_col = f'ofm_{target_type}_chg'
        df.at[index, ofm_chg_col] = row[ofm_base_col] - row[ofm_start_col]

    # fill NA, round and clip to 0 (no negative change)
    df[ofm_chg_col] = df[ofm_chg_col].fillna(0).round(0).clip(lower=0).astype(int)

    # adjust target change by subtracting ofm change, minimum of 0
    chg_adj_col = f'{target_type}_chg_adj'
    chg_col = f'{target_type}_chg'
    df[chg_adj_col] = (df[chg_col] - df[ofm_chg_col]).clip(lower=0)

    # save adjusted targets table
    table_name = f'adjusted_{target_type}_change_targets'
    out_df = df[['target_id','start',chg_col,chg_adj_col]]
    p.save_table(table_name,out_df)



def run_step(context):
    p = Pipeline(settings_path=context['configs_dir'])
    print("Adjusting targets to base year using OFM estimates...")
    adjust_targets(p,'units')
    adjust_targets(p,'total_pop')
    return context