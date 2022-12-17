import pandas as pd
import re
import os
import numpy as np

def get_unique_features(building_name):
    print("========================================================")
    df = pd.read_csv(building_name)
    print("Total number of columns : ",len(df.description))
    unq_features = df.description.unique()
    print("Total number of unique features : ",len(unq_features))
    rooms = {}
    for i,name in enumerate(df.jci_name):
        if name is np.nan:
            continue
        match = re.search('[0-9]{3,5}', name)
        if match is not None:
            room_no = name[match.start():match.end()]
            if room_no in rooms:
                rooms[room_no].append(df.description[i])
            else:
                rooms[room_no] = [df.description[i]]

    print("Total number of rooms in given building : ",len(rooms))
    features_dict = {}
    for r in rooms:
        for feature in rooms[r]:
            if feature in features_dict:
                features_dict[feature] += 1
            else:
                features_dict[feature] = 1
    print("Total number of distinct features available for rooms : ", len(features_dict))

    # print("Features present commonly in all the rooms: ")
    # for f in features_dict:
    #     if features_dict[f] > len(rooms)-20:
    #         print(f, features_dict[f])

    print("========================================================")
    return features_dict


if __name__ == "__main__":
    data_dir = "object_list/results.bak/"
    dir_list = os.listdir(data_dir)
    most_common_features = {}
    for file in dir_list:
        building_name = data_dir + file
        if "objects" in building_name and "lock" not in building_name:
            print(building_name)
            feats = get_unique_features(building_name)
            for f in feats:
                if f in most_common_features:
                    most_common_features[f] += feats[f]
                else:
                    most_common_features[f] = feats[f]
    most_common_features = dict(sorted(most_common_features.items(), key=lambda kv:kv[1],reverse=True))
    for mcf in most_common_features:
        if most_common_features[mcf] > 50:
            print(mcf, most_common_features[mcf])
