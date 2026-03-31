from sqlalchemy import select
from database import sync_engine, session_factory, Base
from models import WorkersOrm

class SyncORM:
    @staticmethod
    def create_tables():
        Base.metadata.drop_all(sync_engine)
        sync_engine.echo=True
        Base.metadata.create_all(sync_engine)
        sync_engine.echo=True

    @staticmethod
    def insert_workers():
        with session_factory() as session:
            worker_grot = WorkersOrm(username="GROT")
            worker_ruslan = WorkersOrm(username="Ruslan")
            session.add_all([worker_grot, worker_ruslan])
            session.flush()
            session.commit()
    
    @staticmethod
    def select_workers():
        with session_factory() as session:
            # worker_id = 1
            # worker_grot = session.get(WorkersOrm, worker_id)
            query = select(WorkersOrm) # SELECT * FROM workers
            result = session.execute(query)
            workers = result.scalars().all()
            print(f"{workers=}")


    @staticmethod
    def update_worker(worker_id: int=2, new_username: str = "solorus"):
        with session_factory() as session:
            worker_solorus = session.get(WorkersOrm, worker_id)
            worker_solorus.username = new_username
            session.refresh(worker_solorus)
            session.commit()

    # @staticmethod
    # async def async_insert_data():
    #     async with async_session_factory() as session:
    #         worker_bobr = WorkersOrm(username="Bobr")
    #         worker_volk = WorkersOrm(username="Volk")
    #         session.add_all([worker_bobr, worker_volk])
    #         await session.commit()