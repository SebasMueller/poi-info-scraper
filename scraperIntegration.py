import POI
import pandas as pd

def str_to_dict(entry):
        try:
            # print(type(eval(entry)))
            return eval(entry)
        except:
            if entry != "":
                print("NON NAN FAILURE:  ", entry)
            return np.nan

def load_file(fileName):
    df = pd.read_csv(fileName, index_col="Unnamed: 0", converters={'PlaceLocation': str_to_dict})
    return df

def retrieve_opening_hours(row):
    lat = float(row.PlaceLocation["lat"])
    lng = float(row.PlaceLocation["lng"])
    trimmedStoreName = trim_store_name(row.PlaceName)
    storeNametoFunctionDict = { "ALDI" : POI.get_aldi_data,
                                "Co-op" : POI.get_coop_data,
                                "EDEKA" : POI.get_edeka_data,
                                "Waitrose & Partners" : POI.get_waitrose_data,
                                "Asda" : POI.get_asda_data,
                                "REWE" : POI.get_rewe_data,
                                "Marks & Spencer" : POI.get_marks_and_spencers_data,
                                "Coles" : POI.get_coles_data,
                                "Carrefour" : POI.get_carrefour_data,
                                "Kaufland" : POI.get_kaufland_data,
                                "Netto" : POI.get_netto_brands_data,
                                "Mercadona" : POI.get_mercadona_data,
                                "Iceland" : POI.get_iceland_data,
                                "Migros" : POI.get_migros_data,
                                "Tesco" : POI.get_tesco_data,
                                "Morrisons" : POI.get_morrisons_data
                            }
    if trimmedStoreName in storeNametoFunctionDict:
        hoursArray = storeNametoFunctionDict[trimmedStoreName](lat, lng)
        if hoursArray:
            row["openingHours"] = hoursArray
        else:
            print(hoursArray, trimmedStoreName)
    return row

def trim_store_name(store_name):
    if "aldi" in store_name.lower():
        new_name = "ALDI"
    elif "coop" in store_name.lower() or "co-op" in store_name.lower():
        new_name = "Co-op"
    elif "edeka" in store_name.lower():
        new_name = "EDEKA"
    elif "waitrose" in store_name.lower():
        new_name = "Waitrose & Partners"
    # elif "waitrose & partners" in store_name.lower() and "little" in store_name.lower():
    #     new_name = "Little Waitrose & Partners"
    elif "asda" in store_name.lower():
        new_name = "Asda"
    elif "rewe" in store_name.lower():
        new_name = "REWE"
    elif ("marks" in store_name.lower() and "spencer" in store_name.lower()) or "m&s" in store_name.lower():
        new_name = "Marks & Spencer"
    elif "coles" in store_name.lower():
        new_name = "Coles"
    elif "carrefour" in store_name.lower():
        new_name = "Carrefour"
    elif "kaufland" in store_name.lower():
        new_name = "Kaufland"
    elif "netto" in store_name.lower():
        new_name = "Netto"
    elif "mercadona" in store_name.lower():
        new_name = "Mercadona"
    elif "iceland" in store_name.lower():
        new_name = "Iceland"
    elif "migros" in store_name.lower():
        new_name = "Migros"
    elif "iceland" in store_name.lower():
        new_name = "Iceland"
    elif "tesco" in store_name.lower():
        new_name = "Tesco"
    elif "morrison" in store_name.lower():
        new_name = "Morrisons"



    elif "lidl" in store_name.lower():
        new_name = "Lidl"
    elif "woolworths" in store_name.lower():
        new_name = "Woolworths"
    elif "walmart" in store_name.lower():
        new_name = "Walmart"
    elif "supermercato" in store_name.lower() or "supermercados" in store_name.lower() or "supermercado" in store_name.lower():
        new_name = "Supermercado"
    elif "spar" in store_name.lower():
        new_name = "SPAR"
    elif "pingo" in store_name.lower():
        new_name = "Pingo"
    elif "one stop" in store_name.lower():
        new_name = "One Stop"
    elif "esselunga" in store_name.lower():
        new_name = "Esselunga"
    elif "e center" in store_name.lower():
        new_name = "E center"
    elif "combi" in store_name.lower():
        new_name = "Combi"
    elif "autoservicio" in store_name.lower():
        new_name = "Autoservicio"
    elif "ah" in store_name.lower():
        new_name = "AH"
    else: new_name = store_name
    return new_name 


storeBase = load_file("contributions.csv")[["PlaceLocation", "PlaceName"]]
print(storeBase.head())
storeBase.iloc[0:200].apply(retrieve_opening_hours, axis=1).to_csv("openingHours.csv")
