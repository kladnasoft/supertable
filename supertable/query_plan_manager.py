import os
import uuid
from datetime import datetime

from supertable.config.defaults import logger
from supertable.utils.helper import generate_hash_uid
from supertable.utils.sql_parser import SQLParser


class QueryPlanManager:
    def __init__(self, super_name: str, organization: str, current_meta_path: str, parser: SQLParser):
        self.identity = "queries"
        self.organization = organization
        self.original_table = parser.original_table

        self.temp_dir = os.path.join(self.organization, super_name, "tmp")
        os.makedirs(self.temp_dir, exist_ok=True)
        logger.debug(f"Created temp dir {self.temp_dir}")

        self.meta_path_used = current_meta_path
        self.query_hash = generate_hash_uid( "|".join([self.meta_path_used, parser.parsed_query]))
        self.query_id = str(uuid.uuid4())
        self.query_plan_id = self.generate_query_plan_filename(alias="plan", extension="json")
        self.query_plan_path = os.path.join(self.temp_dir, self.query_plan_id)

    def generate_query_plan_filename(self, alias, extension="json"):
        utc_timestamp = int(datetime.now().timestamp() * 1000)
        filename = f"{utc_timestamp}_{self.query_hash}_{alias}.{extension}"
        return filename
