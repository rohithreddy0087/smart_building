from typing import Callable, List, Optional, Dict
import numpy as np
import re
import pandas as pd
from config_parser import ConfigFileparser
import os
from config_parser import get_config
import json
from data_fetcher import DataFetcher

class DataLoader:
    def __init__(self,
        config: ConfigFileparser,
        most_common_features: List[str]
    ) -> None:
        self.config = config
        self.most_common_features = most_common_features
        self.dataFetcherObj = DataFetcher(self.config)
        self.__building_data = {}
    
    @property
    def building_data(self)->dict:
        return self.__building_data

    def load_data(self,building_devices_file: str):
        df = pd.read_csv(building_devices_file)
        for i,name in enumerate(df.jci_name):
            if name is np.nan:
                continue
            match = re.search('[0-9]{3,5}', name)
            if match is not None:
                room_no = name[match.start():match.end()]
                for feature in self.most_common_features:
                    if df.description[i] == feature:
                        latest_timestamp = self.dataFetcherObj.fetch_latest_entry(df.uuid[i])
                        initial_timestamp = self.dataFetcherObj.fetch_first_entry(df.uuid[i])
                        if building_devices_file in self.__building_data:
                            if room_no in self.__building_data[building_devices_file]:
                                self.__building_data[building_devices_file][room_no][df.uuid[i]] = {
                                        "feature": feature,
                                        "inital_timestamp": str(initial_timestamp),
                                        "final_timestamp": str(latest_timestamp)
                                }
                            else:
                                self.__building_data[building_devices_file][room_no] = {}
                                self.__building_data[building_devices_file][room_no][df.uuid[i]] = {
                                        "feature": feature,
                                        "inital_timestamp": str(initial_timestamp),
                                        "final_timestamp": str(latest_timestamp)
                                }
                        else:
                            self.__building_data[building_devices_file] = {}
                            self.__building_data[building_devices_file][room_no] = {}
                            self.__building_data[building_devices_file][room_no][df.uuid[i]] = {
                                        "feature": feature,
                                        "inital_timestamp": str(initial_timestamp),
                                        "final_timestamp": str(latest_timestamp)
                                }

                        break
    
    def read_from_csv(self,data_dir :str = "object_list/results.bak/"):
        dir_list = os.listdir(data_dir)
        for file in dir_list:
            building_name = data_dir + file
            if "objects" in building_name and "lock" not in building_name:
                self.load_data(building_name)
            break
    
    def save_dict_as_json(self):
        json.dump( self.__building_data, open( "building_data.json", 'w' ) )


def get_unique_features(building_name: str, config: ConfigFileparser):
    config.logger.debug("========================================================")
    df = pd.read_csv(building_name)
    config.logger.debug(f"Total number of columns : {len(df.description)}")
    unq_features = df.description.unique()
    config.logger.debug(f"Total number of unique features : {len(unq_features)}")
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

    config.logger.debug(f"Total number of rooms in given building : {len(rooms)}")
    features_dict = {}
    for r in rooms:
        for feature in rooms[r]:
            if feature in features_dict:
                features_dict[feature] += 1
            else:
                features_dict[feature] = 1
    config.logger.debug(f"Total number of distinct features available for rooms : {len(features_dict)}")

    # print("Features present commonly in all the rooms: ")
    # for f in features_dict:
    #     if features_dict[f] > len(rooms)-20:
    #         print(f, features_dict[f])

    config.logger.debug("========================================================")
    return features_dict

def extract_most_common_features(dir_list: str, config: ConfigFileparser):
    common_features = {}
    for file in dir_list:
        building_name = data_dir + file
        if "objects" in building_name and "lock" not in building_name:
            config.logger.debug(building_name)
            feats = get_unique_features(building_name,config)
            for f in feats:
                if f in common_features:
                    common_features[f] += feats[f]
                else:
                    common_features[f] = feats[f]
    common_features = dict(sorted(common_features.items(), key=lambda kv:kv[1],reverse=True))
    most_common_features = []
    for mcf in common_features:
        if common_features[mcf] > 50:
            most_common_features.append(mcf)
    return most_common_features

if __name__ == "__main__":
    data_dir = "object_list/results.bak/"
    global_var = {}
    config = get_config(global_var,configfile="config.ini")
    dir_list = os.listdir(data_dir)
    most_common_features = extract_most_common_features(dir_list, config)
    dataLoaderObj = DataLoader(config, most_common_features)
    dataLoaderObj.read_from_csv(data_dir)
    dataLoaderObj.save_dict_as_json()
    config.logger.debug(f"Length {len(dataLoaderObj.building_data)}")
