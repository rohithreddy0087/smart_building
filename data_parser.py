from typing import Callable, List, Optional, Dict, Tuple
from data_fetcher import DataFetcher
from config_parser import ConfigFileparser
import json

class DataParser:
    def __init__(self,
        config: ConfigFileparser,
        meta_json_file : str = "building_data.json"
    ) -> None:
        self.config = config
        self.__meta = json.loads(meta_json_file)
        self.dataFetcherObj = DataFetcher(self.config)
    
    def __get_data_between_interval(self, uuid: str, start_time: str, end_time: str)-> List[Tuple]:
        rows = self.dataFetcherObj.fetch_data_between_interval(uuid,start_time,end_time)
        feature_data = self.parse_feature_data(rows)
        return feature_data

    def __get_data_with_limit(self, uuid: str,limit: int = 10)-> List[Tuple]:
        rows = self.dataFetcherObj.fetch_latest_data(uuid,limit)
        feature_data = self.parse_feature_data(rows)
        return feature_data

    def parse_feature_data(self, data: List[Tuple])->List[dict]:
        data_list = []
        for d in data:
            data_dict = {}
            data_dict["time"] = str(d[0])
            data_dict["value"] = d[1]
            data_list.append(data_dict)
        return data_list

    def create_meta_dict(self,building_name: str, start_time: str, end_time: str, limit: int, features: List[str])->dict:
        meta_dict = {
            "building_name": building_name,
            "start_time": start_time,
            "end_time": end_time,
            "total_rooms": len(self.__meta[building_name]),
            "features": features,
            "line_limit": limit
        }
        return meta_dict

    def create_items_list(self,building_name: str, start_time: str, end_time: str, limit: int, features: List[str])->List[dict]:
        items_list = []
        for room_no in self.__meta[building_name]:
            item_dict = {}
            item_dict["room_number"] = room_no
            item_dict["recorded_features"] = []
            for feature in features:
                feature_dict = {}
                feature_dict["feature"] = feature
                if feature in self.__meta[building_name][room_no]:
                    uuid = self.__meta[building_name][room_no][feature]
                    feature_dict["id"] = uuid[10:]
                    if start_time is not None and end_time is not None:
                        feature_dict["data"] = self.__get_data_between_interval(uuid,start_time,end_time)
                    else:
                        feature_dict["data"] = self.__get_data_with_limit(uuid,limit)
                item_dict["data"].append(feature_dict)
            items_list.append(item_dict)
        
        return items_list


