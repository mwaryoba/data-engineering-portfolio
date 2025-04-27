import yaml, inspect, os, sys
from yaml import SafeLoader
from airflow.models import Variable

def get_config(path=None):
    internal_path = path
    
    # If we aren't passed a path, try to get it from the caller's directory
    if not path:
        #caller = inspect.getframeinfo(sys._getframe(1)).filename
        internal_path = os.path.join(os.path.abspath(os.path.dirname( __file__ )), 'config.yaml')

    # Load config from file and return it with the environment
    configf = open(internal_path)
    return yaml.load(configf, Loader=SafeLoader), Variable.get("environment")