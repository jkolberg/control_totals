# shared calculations for units change and population change targets

def load_input_tables(pipeline,targets_type):
    # targets type: 'units' or 'total_pop'

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
        p.get_table(f'adjusted_{targets_type}_change_targets')
        .merge(dec, on='target_id', how='left')
    )

    # calculate decennial household population, households and household size by rgid
    df['dec_hhpop_by_rgid'] = df.groupby('RGID')['dec_hhpop'].transform('sum')
    df['dec_hh_by_rgid'] = df.groupby('RGID')['dec_hh'].transform('sum')
    df['dec_hhsz_by_rgid'] = df['dec_hhpop_by_rgid'] / df['dec_hh_by_rgid']

    # calculate decennial hhsz
    df['dec_hhsz'] = df['dec_hhpop'] / df['dec_hh']

    return df, dec


def calc_gq(pipeline, df, dec):
    p = pipeline
    target_horizon_year = p.settings['target_horizon_year']
    horizon_cols = TargetHorizonColumnNames(p)

    # load the Regional Economic Forecast table to get total GQ for the region in the horizon year
    ref = p.get_table('ref_projection')
    reg_gq_horizon = ref.loc[ref.variable == 'GQ Pop', str(target_horizon_year)].item()

    # calculate GQ percentage of the region based on decennial data
    reg_dec_gq_sum = dec['dec_gq'].sum()
    df['dec_gq_pct'] = df['dec_gq'] / reg_dec_gq_sum

    # calculate target area GQ for horizon year as a percentage of the regional GQ from REF
    gq_horizon_col = horizon_cols.gq()
    df[gq_horizon_col] = (df['dec_gq_pct'] * reg_gq_horizon).round(0).astype(int)
    return df


class TargetHorizonColumnNames:
    def __init__(self, pipeline):
        self.target_horizon_year = pipeline.settings['target_horizon_year']

    def units(self):
        return f'units_{self.target_horizon_year}'

    def households(self):
        return f'hh_{self.target_horizon_year}'
    
    def hhsz(self):
        return f'hhsz_{self.target_horizon_year}'
    
    def hhpop(self):
        return f'hhpop_{self.target_horizon_year}'

    def hhpop_initial(self):
        return f'hhpop_initial_{self.target_horizon_year}'

    def hhpop_factored(self):
        return f'hhpop_factored_{self.target_horizon_year}'

    def total_pop(self):
        return f'total_pop_{self.target_horizon_year}'

    def gq(self):
        return f'gq_{self.target_horizon_year}'
