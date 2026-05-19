from dotenv import load_dotenv
import os
from logging import basicConfig, INFO, getLogger, Logger
from .config import Config
from .src.utils.supabase import SupabaseClient

load_dotenv()

# Configure logging
basicConfig(
    level=INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)
log: Logger = getLogger(__name__)

# Initialize Supabase
supabase = SupabaseClient(url=os.environ.get("SUPABASE_URL"), key=os.environ.get("SUPABASE_KEY"))

# Create the single, shared config instance
config = Config(supabase=supabase, env=os.environ)