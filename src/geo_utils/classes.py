import reverse_geocoder as rg

def get_country_codes_from_latlng_df(df, lat_col='latitude', lng_col='longitude', get_state=True, get_city=True):
    revg_input = df[[lat_col, lng_col]].dropna().drop_duplicates()
    if len(revg_input) > 0:
        rg_results = rg.search(list(zip(revg_input[lat_col], revg_input[lng_col])))
        revg_input['country_code'] = [i['cc'] for i in rg_results]
        if get_state:
            revg_input['state'] = [i['admin1'] for i in rg_results]
        if get_city:
            revg_input['city'] = [i['name'] for i in rg_results]

    return df.merge(revg_input, how="left", on=[lat_col, lng_col])

