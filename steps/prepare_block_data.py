import pandas as pd
from util import Pipeline


def sum_decennial_by_control_area(pipeline):
    p = pipeline
    dec = p.get_table('dec_block_data')
    blk = p.get_table('block_control_xwalk')
    block_id = p.get_id_col('blocks')

    # merge decennial data with block to control area crosswalk
    df = dec.merge(blk, left_on='geoid', right_on=block_id)

    # get list of decennial census columns
    dec_cols = list(p.settings['census_variables'].keys())

    # sum decennial data by control area
    dec_by_control = (
        df.groupby('control_id')
        .sum()[dec_cols]
        .astype(int)
        .reset_index()
    )

    # save to HDF5
    p.save_table('decennial_by_control_area', dec_by_control)

def sum_ofm_by_control_area(pipeline):
    p = pipeline
    for ofm_table in ['ofm_estimates_prev_year', 'ofm_estimates_census_year']:
        ofm = p.get_table(ofm_table)
        ofm_block_id = p.get_id_col(ofm_table)
        blk = p.get_table('block_control_xwalk')
        block_id = p.get_id_col('blocks')

        # merge ofm data with block to control area crosswalk
        df = ofm.merge(blk, left_on=ofm_block_id, right_on=block_id)

        # sum ofm data by control area
        ofm_by_control = (
            df.groupby('control_id')
            .sum()[['housing_units', 'occupied_housing_units', 
                    'group_quarters_population', 'household_population']]
            .reset_index()
        )

        # save to HDF5
        p.save_table(f'{ofm_table}_by_control_area', ofm_by_control)

def run_step(context):
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print("Aggregating Decennial Census data to control area level...")
    sum_decennial_by_control_area(p)
    print("Aggregating OFM estimates data to control area level...")
    sum_ofm_by_control_area(p)
    return context