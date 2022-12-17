from typing import Callable, List, Tuple, Dict
from data_parser import DataParser
from config_parser import ConfigFileparser
from config_parser import get_config
import json

class DataInterface:
    def __init__(self,
        config: ConfigFileparser,
    ) -> None:
        self.config = config
        self.dataParserObj = DataParser(self.config)
        self.__meta_dict = {}
        self.__items_list = []

    @property
    def meta_dict(self)->dict:
        return self.__meta_dict
    
    @property
    def items_list(self)->dict:
        return self.__items_list
    

    def fetch_and_save_data(self, building_name: str, start_time: str = None, end_time: str = None, limit: int = 100, features: List[str] = None)->dict:
        self.__meta_dict = self.dataParserObj.create_meta_dict(building_name,start_time,end_time,limit,features)
        self.__items_list = self.dataParserObj.create_items_list(building_name,start_time,end_time,limit,features)
        json_dict = {
            "meta": self.__meta_dict,
            "items": self.__items_list
        }
        json.dump( ret, open( f"{building_name}.json", 'w' ) )
        return json_dict

if __name__ == "__main__":
    global_var = {}
    config = get_config(global_var,configfile="config.ini")
    dataInterfaceObj = DataInterface(config)
    building_name = ""
    start_time = ""
    end_time = ""
    features = []
    latest_line_limit = int("")

    ret = dataInterfaceObj.get_data(inp_uuid,10)
