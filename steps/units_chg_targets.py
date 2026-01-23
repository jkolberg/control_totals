import pandas as pd
from util import Pipeline


def load_input_tables(pipeline):
    p = pipeline
    # load control to target lookup
    control_target_lookup = p.get_table('control_target_lookup')

    # sum decennial data to target areas
    dec = (
        p.get_table('decennial_by_control_area')
        .merge(control_target_lookup, on='control_id', how='left')
        .groupby(['target_id','RGID','county_id']).sum().reset_index()
        .drop(columns=['control_id','name'])
    )

    # merge decennial data with adjusted targets
    df = (
        p.get_table('adjusted_units_change_targets')
        .merge(dec, on='target_id', how='left')
    )

    # calculate decennial household population, households and household size by rgid
    df['dec_hhpop_by_rgid'] = df.groupby('RGID')['dec_hhpop'].transform('sum')
    df['dec_hh_by_rgid'] = df.groupby('RGID')['dec_hh'].transform('sum')
    df['dec_hhsz_by_rgid'] = df['dec_hhpop_by_rgid'] / df['dec_hh_by_rgid']

    # calculate decennial hhsz
    df['dec_hhsz'] = df['dec_hhpop'] / df['dec_hh']

    return df, dec

def load_hhsz_vacancy_rates(pipeline):
    p = pipeline
    # load hard coded king county hhsz and vacancy rates
    king_hhsz = p.settings['king_hhsz']
    king_vac_rates = p.settings['king_vac']
    return king_hhsz, king_vac_rates

def get_units_horizon_col(pipeline):
    p = pipeline
    target_horizon_year = p.settings['target_horizon_year']
    return f'units_{target_horizon_year}'

def get_households_horizon_col(pipeline):
    p = pipeline
    target_horizon_year = p.settings['target_horizon_year']
    return f'hh_{target_horizon_year}'

def get_hhpop_init_horizon_col(pipeline):
    p = pipeline
    target_horizon_year = p.settings['target_horizon_year']
    return f'hhpop_initial_{target_horizon_year}'

def get_hhpop_factored_horizon_col(pipeline):
    p = pipeline
    target_horizon_year = p.settings['target_horizon_year']
    return f'hhpop_factored_{target_horizon_year}'

def get_total_pop_horizon_col(pipeline):
    p = pipeline
    target_horizon_year = p.settings['target_horizon_year']
    total_pop_horizon_col = f'totalpop_{target_horizon_year}'
    return total_pop_horizon_col

def get_gq_horizon_col(pipeline):
    p = pipeline
    target_horizon_year = p.settings['target_horizon_year']
    gq_horizon_col = f'gq_{target_horizon_year}'
    return gq_horizon_col

def calc_by_rgid(pipeline, targets_df):
    p = pipeline
    # calculations by rgid: takes targets by target area and sums to rgid level
    
    #group by rgid to get totals for each rgid
    df = targets_df.drop(columns=['target_id','start','county_id']).groupby('RGID').sum().reset_index()

    # load hard coded king county hhsz and vacancy rates
    king_hhsz, king_vac_rates = load_hhsz_vacancy_rates(p)

    # add hhsz and vacancy rate using hard coded rates from settings.yaml
    df['hhsz'] = df['RGID'].map(king_hhsz)
    df['vacancy_rate'] = df['RGID'].map(king_vac_rates)

    # load target horizon year and set column names for target horizon year
    units_horizon_col = get_units_horizon_col(p)
    households_horizon_col = get_households_horizon_col(p)
    hhpop_init_horizon_col = get_hhpop_init_horizon_col(p)
    hhpop_factored_horizon_col = get_hhpop_factored_horizon_col(p)

    # calcuate horizon year units
    df[units_horizon_col] = df['dec_units'] + df['units_chg_adj']

    # calculate horizon year households
    df[households_horizon_col] = (df[units_horizon_col] * (1 - df['vacancy_rate']/100)).round(0).astype(int)

    # calculate horizon year initial household population (unfactored)
    df[hhpop_init_horizon_col] = (df[households_horizon_col] * df['hhsz']).round(0).astype(int)

    # load total household population target for horizon year
    hhpop_horizon_forced_total = p.settings['king_hhpop_2044']

    # factor household population to match target
    hhpop_horizon_sum = df[hhpop_init_horizon_col].sum()
    hhpop_factor = hhpop_horizon_forced_total / hhpop_horizon_sum
    df[hhpop_factored_horizon_col] = (df[hhpop_init_horizon_col] * hhpop_factor).round(0).astype(int)
    return df


def calc_by_target_area(pipeline, df, targets_rgid):
    p = pipeline
    # calculations by target areas

    # load hard coded king county hhsz and vacancy rates
    king_hhsz, king_vac_rates = load_hhsz_vacancy_rates(p)

    # get adjusted household sizes for king metro areas
    king_metro_adj_hhsz = p.settings['king_metro_adj_hhsz']
    # replace metro value in king county hhsz dict
    king_hhsz_adj = king_hhsz.copy()
    king_hhsz_adj[1] = king_metro_adj_hhsz
    # map adjusted hhsz to df
    df['king_hhsz'] = df['RGID'].map(king_hhsz_adj)

    # map vacancy rates to df
    df['vacancy_rate'] = df['RGID'].map(king_vac_rates)

    # load target horizon year and set column names for target horizon year
    target_horizon_year = p.settings['target_horizon_year']
    units_horizon_col = get_units_horizon_col(p)
    households_horizon_col = get_households_horizon_col(p)
    hhpop_init_horizon_col = get_hhpop_init_horizon_col(p)
    hhpop_factored_horizon_col = get_hhpop_factored_horizon_col(p)

    # calculate units for target horizon year
    df[units_horizon_col] = df['units_chg_adj'] + df['dec_units']

    # calculate households for target horizon year
    df[households_horizon_col] = (df[units_horizon_col] * (1 - df['vacancy_rate']/100)).round(0).astype(int)

    # calculate adjusted hhsz
    df['hhsz'] = df['king_hhsz'] / df['dec_hhsz_by_rgid'] * df['dec_hhsz']

    # if hhsz is greater than 5, use original value
    df.loc[df.hhsz>5, 'hhsz'] = df.loc[df.hhsz>5, 'king_hhsz']

    # calculate initial hhpop for target horizon year
    df[hhpop_init_horizon_col] = (df[households_horizon_col] * df['hhsz']).round(0).astype(int)

    # sum initial hhpop by rgid and add as a column
    hhpop_horizon_sum_by_rgid_col = f'initial_hhpop_{target_horizon_year}_sum_by_rgid'
    df[hhpop_horizon_sum_by_rgid_col] = df.groupby('RGID')[hhpop_init_horizon_col].transform('sum')

    # merge factored hhpop by rgid to df
    df = (
        df.merge(targets_rgid[['RGID', hhpop_factored_horizon_col]]
                .rename(columns={
                    hhpop_factored_horizon_col: f'hhpop_rgid_factored_{target_horizon_year}'
                    }), on='RGID', how='left')
    )

    # calculate factored hhpop for target horizon year
    df['hhpop_factor'] = df[f'hhpop_rgid_factored_{target_horizon_year}'] / df[hhpop_horizon_sum_by_rgid_col]
    df[hhpop_factored_horizon_col] = (df[hhpop_init_horizon_col] * df['hhpop_factor']).round(0).astype(int)
    return df

def calc_gq_tot_pop(pipeline, df, dec):
    p = pipeline
    # load target horizon year and set column names for target horizon year
    target_horizon_year = p.settings['target_horizon_year']
    units_horizon_col = get_units_horizon_col(p)
    households_horizon_col = get_households_horizon_col(p)
    hhpop_init_horizon_col = get_hhpop_init_horizon_col(p)
    hhpop_factored_horizon_col = get_hhpop_factored_horizon_col(p)
    
    # load the Regional Economic Forecast table to get total GQ for the region in the horizon year
    ref = p.get_table('ref_projection')
    reg_gq_horizon = ref.loc[ref.variable == 'GQ Pop', str(target_horizon_year)].item()

    # calculate GQ percentage of the region based on decennial data
    reg_dec_gq_sum = dec['dec_gq'].sum()
    df['dec_gq_pct'] = df['dec_gq'] / reg_dec_gq_sum

    # calculate target area GQ for horizon year as a percentage of the regional GQ from REF
    gq_horizon_col = get_gq_horizon_col(p)
    df[gq_horizon_col] = (df['dec_gq_pct'] * reg_gq_horizon).round(0).astype(int)

    # add GQ to household population to get total population for horizon year
    total_pop_horizon_col = get_total_pop_horizon_col(p)
    df[total_pop_horizon_col] = df[hhpop_factored_horizon_col] + df[gq_horizon_col]
    return df


def calculate_targets(pipeline):
    p = pipeline
    df, dec = load_input_tables(p)
    targets_rgid = calc_by_rgid(p, df)
    df = calc_by_target_area(p, df, targets_rgid)
    df = calc_gq_tot_pop(p, df, dec)
    # save table
    p.save_table('adjusted_units_change_targets',df)

def run_step(context):
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print('Calculating targets for counties that use housing targets...')
    calculate_targets(p)
    return context
