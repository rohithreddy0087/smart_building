import psycopg2
from typing import Callable, List, Optional, Dict, Tuple
import numpy as np
from config_parser import ConfigFileparser
import re
import pandas as pd


class DataFetcher:
    def __init__(self,
        config: ConfigFileparser
    ) -> None:
        self.config = config
        self.psql_conn,self.psql_cur = self.create_psql()
        self.most_common_features = ["Zone Temperature","Actual Cooling Setpoint","Cooling Command",\
        "Actual Heating Setpoint","Common Setpoint","Heating Valve Command","Occupancy Status"]
        self.uuids = {}

    def create_psql(self):
        psql_conn = psycopg2.connect(user = self.config.db_user,
                                password = self.config.db_pass,
                                host = self.config.db_host,
                                port = str(self.config.db_port),
                                database = self.config.db_name)
        psql_cur = psql_conn.cursor()
        return psql_conn, psql_cur

    def fetch_data(self,query: str)->List[Tuple]:
        self.psql_cur.execute(query)
        rows = self.psql_cur.fetchall()
        return rows

    def fetch_data_between_interval(self,uuid: str, start_time: str, end_time: str)->List[Tuple]:
        query = "select time,number from %s where uuid::text = '%s' and \
                    time between '%s' and '%s'"%(self.config.db_table, uuid, start_time, end_time)
        data = self.fetch_data(query)
        return data

    def fetch_latest_data(self,uuid: str, limit : int = 100)->List[Tuple]:
        query = "select time,number from %s where uuid::text = '%s' limit %s"%(self.config.db_table, uuid, limit)
        data = self.fetch_data(query)
        return data

    def fetch_latest_entry(self,uuid: str)->List[Tuple]:
        query = "select time,number from %s where uuid::text = '%s' order by time desc limit 1"%(self.config.db_table, uuid)
        print(query)
        data = self.fetch_data(query)
        if len(data)>0:
            return data[0][0]
        else:
            return None

    def fetch_first_entry(self,uuid: str)->List[Tuple]:
        query = "select time,number from %s where uuid::text = '%s' order by time asc limit 1"%(self.config.db_table, uuid)
        print(query)
        data = self.fetch_data(query)
        if len(data)>0:
            return data[0][0]
        else:
            return None

