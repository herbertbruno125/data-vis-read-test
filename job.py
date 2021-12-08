from datetime import timedelta
from sys import argv

from pyspark.sql.functions import *

from default_args import DefaultArgs
from utils import get_session


class Job(DefaultArgs):

    def __init__(self, environment, dt) -> None:
        super().__init__(environment, dt)
        self.spark = get_session()

    def get_min_proc(self):
        return self.dt - timedelta(days=7)

    def runner(self):
        mutable_df = self.spark.read.format("org.apache.spark.sql.cassandra") \
            .options(table='communication', keyspace='mutable_date').load()

        mutable_df = mutable_df.filter(col('customer') == 1)

        behavior_df = self.spark.read.parquet(f's3a://raw_data_sm/behavior/customer')

        behavior_df = behavior_df.filter(col('date') >= self.get_min_proc() & col('date') <= self.dt)

        person_behavior = behavior_df.select('person_id').distinct().collect()

        person_behavior_values = [s[0] for s in person_behavior]

        person_communication = mutable_df.filter(col('personid').isin(person_behavior_values)).collect()

        person_communication_df


if __name__ == '__main__':
    env = argv[1]
    date_proc = argv[2]
    job = Job(env, date_proc)
    job.runner()
