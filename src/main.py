from queries.orm import SyncORM

SyncORM.create_tables()
# SyncCore.create_tables()

SyncORM.insert_workers()

# SyncCore.select_workers()
# SyncCore.update_worker()

SyncORM.select_workers()
SyncORM.update_worker()
SyncORM.insert_resumes()
SyncORM.select_resumes_avg_salary()