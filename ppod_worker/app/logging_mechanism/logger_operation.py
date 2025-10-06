import logging
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv
import os

load_dotenv()
project_root = os.getenv("Project_Root")
log_dir = os.path.join(os.path.dirname(__file__), "error_in_data_process.log")

# logging.basicConfig(
#    level=logging.INFO,
#    format='%(asctime)s - %(levelname)s - %(message)s',
#    handlers=[
#        logging.FileHandler("error_in_data_process.log"),
#        logging.StreamHandler()
#    ]
# )

rotating_handler = RotatingFileHandler(log_dir, maxBytes=5_000_000, backupCount=3)
rotating_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(rotating_handler)
logger.addHandler(logging.StreamHandler())
