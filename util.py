import json
import pprint

# with open("gcs_destination_ibm_data.json", "r") as fp:
#     data = json.load(fp)

def flat_json_data(data:dict) -> list:
    symbol = data.get("Meta Data",{}).get("2. Symbol",None)
    tsd = data.get("Time Series (Daily)", {})
    ls_data = []

    for date_v, val in tsd.items():
        new_dict = {}
        
        for k, v in val.items():
            new_key = k[3:] 
            new_dict[new_key] = v
        
        new_dict["symbol"] = symbol
        new_dict["date"] = date_v
        new_dict["close"] = float(new_dict["close"])
        new_dict["high"] = float(new_dict["high"])
        new_dict["low"] = float(new_dict["low"])
        new_dict["open"] = float(new_dict["open"])
        new_dict["volume"] = int(float(new_dict["volume"]))
        new_dict["adjusted_close"] = float(new_dict.get("adjusted close", 0.0))
        new_dict["dividend_amount"] = float(new_dict.get("dividend amount", 0.0))
        new_dict["split_coefficient"] = float(new_dict.get("split coefficient", 0.0))

        new_dict.pop("adjusted close",None)
        new_dict.pop("dividend amount",None)
        new_dict.pop("split coefficient",None)

        ls_data.append(new_dict)
    return ls_data

# pprint.pprint(ls_data)