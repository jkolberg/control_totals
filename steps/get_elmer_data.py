from util.elmer_helpers import read_from_elmer_geo, read_from_elmer
from util import Pipeline


def copy_elmer_geo_to_hdf5(pipeline):
    for file in pipeline.get_elmer_geo_list():
        gdf = read_from_elmer_geo(file['sql_table'],file['columns'])
        pipeline.save_geodataframe(file['name'], gdf)

def copy_elmer_to_hdf5(pipeline):
    for table in pipeline.get_elmer_list():
        df = read_from_elmer(table['sql_table'],['*'])
        pipeline.save_table(table['name'], df)

def run_step(context):
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print("Getting ElmerGeo data and saving to HDF5...")
    copy_elmer_geo_to_hdf5(p)
    print("Getting Elmer data and saving to HDF5...")
    copy_elmer_to_hdf5(p)
    return context
