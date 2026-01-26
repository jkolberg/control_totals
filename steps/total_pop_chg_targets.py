import pandas as pd
from util import Pipeline, load_input_tables, calc_gq


def calc_dec_hhsz(dec):
    return dec['dec_hhpop'].sum() / dec['dec_hh'].sum()

def calc_ref_horizon_hhsz(pipeline):
    p = pipeline
    horizon_year = p.settings['targets_end_year']
    ref = p.get_table('ref_projection')
    hhpop = ref.loc[ref.variable == 'HH Pop', str(horizon_year)].item()
    hh = ref.loc[ref.variable == 'HH', str(horizon_year)].item()
    return hhpop / hh

def calc_horizon_hhsz(df, dec_hhsz, ref_hhsz, hhsz_horizon_col):
    # Calculate horizon year household size
    df[hhsz_horizon_col] = (ref_hhsz / dec_hhsz * df['dec_hhsz']).fillna(0)
    # if hhsz is greater than 5 or equal to 0, set to REF hhsz
    df.loc[(df[hhsz_horizon_col]>5) | (df[hhsz_horizon_col]==0) , hhsz_horizon_col] = ref_hhsz
    return df

def calculate_targets(pipeline):
    p = pipeline
    # Get target column names
    targets_end_year = p.settings["targets_end_year"]
    hhpop_horizon_col = f'hhpop_{targets_end_year}'
    gq_horizon_col = f'gq_{targets_end_year}'
    hhsz_horizon_col = f'hhsz_{targets_end_year}'
    hh_horizon_col = f'hh_{targets_end_year}'
    # Load input tables
    df, dec = load_input_tables(p, 'total_pop')

    # Calculate total population for horizon year
    total_pop_horizon_col = f'total_pop_{targets_end_year}'
    df[total_pop_horizon_col] = df['dec_total_pop'] + df['total_pop_chg_adj']

    # Calculate GQ and household population for horizon year
    df = calc_gq(p, df, dec, targets_end_year)
    df[hhpop_horizon_col] = df[total_pop_horizon_col] - df[gq_horizon_col]
    
    # Calculate household size: pct change from decennial to REF applied to decennial hhsz
    dec_hhsz = calc_dec_hhsz(dec)
    ref_hhsz = calc_ref_horizon_hhsz(p)
    df = calc_horizon_hhsz(df, dec_hhsz, ref_hhsz, hhsz_horizon_col)

    # Calculate households for horizon year
    df[hh_horizon_col] = (df[hhpop_horizon_col] / df[hhsz_horizon_col]).round(0).astype(int)
    
    # Save table
    p.save_table('adjusted_total_pop_change_targets', df)


def run_step(context):
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print('Calculating targets for counties that use population targets...')
    calculate_targets(p)
    return context