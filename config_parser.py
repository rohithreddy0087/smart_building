from configparser import ConfigParser
import logging 

class ConfigFileparser:
    """
    Parses configfile and stores them in attributes
    """
    def __init__(self, configfile = "configfile.ini"):
        parser = ConfigParser()
        parser.read(configfile)
        self.db_host = parser.get('DATABASE','DB_HOST')
        self.db_port = int(parser.get('DATABASE','DB_PORT'))
        self.db_name = parser.get('DATABASE','DB_NAME',fallback="brick")
        self.db_user = parser.get('DATABASE','DB_USER')
        self.db_pass = parser.get('DATABASE','DB_PASS')
        self.db_table = parser.get('DATABASE','DB_TABLE')
        FORMAT = '%(asctime)s %(message)s'
        logging.basicConfig(filename='smartbuilding_debug.log',format=FORMAT, level=logging.DEBUG)
        self.logger = logging.getLogger('SMART_BUILDING')

def get_config(global_var,configfile="config.ini"):
    """Creates an instance of ConfigFileparser and stores it in the global_var dictionary
    Args:
        global_var (dict): to store all the classes initated
        configfile (str): path to config file

    Returns:
        ConfigFileparser: Instance of ConfigFileparser class
    """
    if "config" not in global_var:
        global_var["config"] = ConfigFileparser(configfile)
    return global_var["config"]