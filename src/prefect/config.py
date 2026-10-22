from os.path import join, dirname
from dotenv import load_dotenv
import os

dotenv_path = join(dirname(dirname(dirname(__file__))), '.env')
load_dotenv(dotenv_path)

class Config:

    BASE_URL   = os.environ.get("BASE_URL", "")
    PREFIX_URL = os.environ.get("PREFIX_URL", "")
