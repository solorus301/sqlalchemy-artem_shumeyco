import asyncio
import os
import sys
from queries.orm import create_tables, insert_data, async_insert_data

create_tables()
insert_data()

# asyncio.run(async_insert_data())