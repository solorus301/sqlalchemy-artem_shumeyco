from queries.core import SyncCore
from queries.orm import SyncORM


SyncORM.create_tables()

SyncORM.insert_workers()

# SyncCore.select_workers()
# SyncCore.update_worker()

SyncORM.select_workers()
SyncORM.update_worker()