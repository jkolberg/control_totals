import pandas as pd
from util import Pipeline, calc_gq


def load_targets_tables(pipeline):
    p = pipeline

    unit_change_targets = p.get_table('adjusted_units_change_targets')
    pop_change_targets = p.get_table('adjusted_total_pop_change_targets')
    return pd.concat([unit_change_targets, pop_change_targets], ignore_index=True)


def extrapolate_target(pipeline,df,col):
    # col is either 'hh' or 'total_pop'
    p = pipeline
    base_year = p.settings['base_year']
    targets_end_year = p.settings['targets_end_year']
    controls_end_year = p.settings['end_year']

    # extrapolate to controls horizon year based on the avg annual change from base year to targets horizon year
    years_to_target = targets_end_year - base_year
    years_to_control = controls_end_year - base_year
    annual_change_col = f'{col}_annual_change'
    dec_base_col = f'dec_{col}'
    target_col = f'{col}_{targets_end_year}'
    df[annual_change_col] = (df[target_col] - df[dec_base_col]) / years_to_target
    control_col = f'{col}_{controls_end_year}'
    df[control_col] = df[dec_base_col] + df[annual_change_col] * years_to_control
    df[control_col] = df[control_col].round(0).fillna(0).astype(int)

    return df


def extrapolate_to_controls_year(pipeline):
    p = pipeline
    # get controls horizon year from settings.yaml
    controls_end_year = p.settings['end_year']
    # load targets tables
    df = load_targets_tables(p)
    # extrapolate hh and total_pop to controls horizon year
    df = extrapolate_target(p,df,'hh')
    df = extrapolate_target(p,df,'total_pop')
    # calculate gq for controls horizon year
    dec = df[['target_id','dec_gq']]
    df = calc_gq(p,df,dec,controls_end_year)
    # calculate hhpop for controls horizon year
    df[f'hhpop_{controls_end_year}'] = df[f'hh_{controls_end_year}'] + df[f'total_pop_{controls_end_year}']
    # save table
    p.save_table('extrapolated_targets', df)

def run_step(context):
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    controls_end_year = p.settings['end_year']
    targets_end_year = p.settings['targets_end_year']
    print(f'Extrapolating from targets end year ({targets_end_year}) to control total end year ({controls_end_year})...')
    extrapolate_to_controls_year(p)
    return context