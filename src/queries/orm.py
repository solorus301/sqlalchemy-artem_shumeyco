from sqlalchemy import insert, select, func, cast, Integer, and_
from sqlalchemy.orm import aliased
from database import sync_engine, session_factory, Base
from models import WorkersOrm, ResumesOrm, Workload

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

    @staticmethod
    def insert_resumes():
        with session_factory() as session:
            resume_jack_1 = ResumesOrm(
                title="Python Junior Developer",
                salary=50000,
                workload=Workload.fulltime, worker_id=1
            )
            resume_jack_2 = ResumesOrm(
                title="Python Разработчик",
                salary=150000,
                workload=Workload.fulltime, worker_id=1
            )
            resume_michael_1 = ResumesOrm(
                title="Python Data Engineer",
                salary=250000,
                workload=Workload.parttime, worker_id=2
            )
            resume_michael_2 = ResumesOrm(
                title="Data Scientist",
                salary=300000,
                workload=Workload.fulltime, worker_id=2
            )
            session.add_all([resume_jack_1, resume_jack_2, resume_michael_1, resume_michael_2])
            session.commit()

# select workload, avg(salary) as avg_salary
# from resumes
# where title like '%Python%' and salary > 40000
# group by workload

    @staticmethod
    def select_resumes_avg_salary(like_language: str = "Python"):
        with session_factory() as session:
            query = (
                select(
                    ResumesOrm.workload,
                    cast(func.avg(ResumesOrm.salary), Integer).label("avg_salary"),
                )
                .select_from(ResumesOrm)
                .filter(and_(
                    ResumesOrm.title.contains(like_language),
                    ResumesOrm.salary > 40000,
                ))
                .group_by(ResumesOrm.workload)
                .having(cast(func.avg(ResumesOrm.salary), Integer) > 70000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True}))
            res = session.execute(query)
            result = res.all()
            print(result)

    @staticmethod
    def insert_additional_resumes():
        with session_factory() as session:
            workers = [
                {"username": "vital"},  # id 3
                {"username": "clavik"},  # id 4
                {"username": "artur"},
            ]
            resumes = [
                {"title": "Python программист(долбаеб)", "salary": 60000, "workload": "fulltime", "worker_id": 3},
                {"title": "Machine Learning Engineer", "salary": 70000, "workload": "parttime", "worker_id": 3},
                {"title": "Python Data Scientist", "salary": 80000, "workload": "parttime", "worker_id": 4},
                {"title": "Python Analyst", "salary": 90000, "workload": "fulltime", "worker_id": 4},
                {"title": "Python Junior Developer", "salary": 100000, "workload": "fulltime", "worker_id": 5},
            ]
            insert_workers = insert(WorkersOrm).values(workers)
            insert_resumes = insert(ResumesOrm).values(resumes)
            session.execute(insert_workers)
            session.execute(insert_resumes)
            session.commit()

    @staticmethod
    def join_cte_subquery_window_func(like_language: str = "Python"):
        '''        
                WITH helper2 as (
            SELECT *, salary-avg_workload_salary as salary_diff
            FROM
            (SELECT
                w.id,
                w.username,
                r.salary,
                r.workload,
                avg(r.salary) OVER (PARTITION BY workload)::int AS avg_workload_salary
            FROM resumes r
            JOIN workers w ON r.worker_id = w.id) helper1
        )
        SELECT * FROM helper2
        ORDER BY salary_diff DESC
        '''
        with session_factory() as session:
            r = aliased(ResumesOrm)
            w = aliased(WorkersOrm)
            subq = (
                select(
                    w.id,
                    w.username,
                    r.salary,
                    r.workload,
                    func.avg(r.salary).over(partition_by=r.workload).cast(Integer).label("avg_workload_salary"),
                )
                # .select_from(r)
                .join(r, r.worker_id == w.id)
                .subquery("helper1")
            )
            
            cte = (
                select(
                    subq.c.id,
                    subq.c.username,
                    subq.c.salary,
                    subq.c.workload,
                    subq.c.avg_workload_salary,
                    (subq.c.salary - subq.c.avg_workload_salary).label("salary_diff"),
                )
                .cte("helper2")
            )
            query = (
                select(cte)
                .order_by(cte.c.salary_diff.desc())
            )

            res = session.execute(query)
            result = res.all()
            print(f"{result=}")
            # print(query.compile(compile_kwargs={"literal_binds": True}))
