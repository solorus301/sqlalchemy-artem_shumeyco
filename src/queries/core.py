from sqlalchemy import text, insert, select, update
from database import sync_engine, async_engine 
from models import metadata_obj, workers_table

def sync_get_conn():
    with sync_engine.connect() as conn:
        res = conn.execute(text("SELECT VERSION()"))
        print(f"{res.first()=}")

async def async_get_conn():

    async with async_engine.connect() as conn:
        res = await conn.execute(text("SELECT VERSION()"))
        print(f"{res.first()=}")

class SyncCore:
    @staticmethod
    def create_tables():
        sync_engine.echo=False
        metadata_obj.drop_all(sync_engine)
        metadata_obj.create_all(sync_engine)
        sync_engine.echo=True

    @staticmethod
    def insert_workers():
        with sync_engine.connect() as conn:
            stmt = insert(workers_table).values(
                [
                    {"username": "GROT"},
                    {"username": "Ruslan"},
                ]
            )
            conn.execute(stmt)
            conn.commit()
            
    @staticmethod
    def select_workers():
        with sync_engine.connect() as conn:
            query = select(workers_table) # SELECT * FROM workers
            result = conn.execute(query)
            workers = result.all()
            print(f"{workers=}")

    @staticmethod
    def update_worker(worker_id: int=2, new_username: str = "solorus"):
        with sync_engine.connect() as conn:
            # stmt = text("UPDATE workers SET username=:username WHERE id=:id")
            # stmt = stmt.bindparams(username=new_username, id=worker_id)
            stmt = (
                update(workers_table)
                .values(username=new_username)
                # .where(workers_table.c.id==worker_id)
                .filter_by(id=worker_id)

            )
            conn.execute(stmt)
            conn.commit()


