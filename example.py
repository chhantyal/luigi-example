import csv
import luigi
import sqlite3

from contextlib import contextmanager

from luigi.contrib.gcs import GCSTarget, GCSClient


@contextmanager
def db_connect(db):
    conn = sqlite3.connect(db)
    try:
        yield conn
    finally:
        conn.close()


class DumpDatabaseTask(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        return luigi.local_target.LocalTarget(f"resources/csv/{self.date.isoformat()}.csv")

    def run(self):
        with db_connect("resources/sales.db") as conn, self.output().open("w") as f:
            cursor = conn.cursor()
            rows = cursor.execute(f"SELECT * FROM sales WHERE date_ordered='{self.date.isoformat()}'")
            writer = csv.writer(f)
            writer.writerow(["customer_id", "date_ordered", "order_value"])

            for row in rows:
                writer.writerow(row)


class UploadToGCSTask(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return DumpDatabaseTask(date=self.date)

    def output(self):
        return GCSTarget(f"gs://luigi_example/{self.date.isoformat()}.csv")

    def run(self):
        client = GCSClient()
        source_path = f"resources/csv/{self.date.isoformat()}.csv"
        client.put(source_path, dest_path=f"gs://luigi_example/{self.date.isoformat()}.csv")


class LoadToAnalyticsDBTask(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return DumpDatabaseTask(date=self.date)

    def run(self):
        with self.input().open("r") as f, db_connect("resources/analytics.db") as conn:
            cursor = conn.cursor()
            reader = csv.DictReader(f)
            rows = [(i['customer_id'], i['date_ordered'], i["order_value"]) for i in reader]
            cursor.executemany(
                "INSERT INTO analytics (customer_id, date_ordered, order_value) VALUES (?, ?, ?);",
                rows)

            with self.output().open("w") as t_file:
                t_file.write("SUCCESS")
            conn.commit()

    def output(self):
        return luigi.local_target.LocalTarget(f"target_files/ingest_{self.date.isoformat()}.txt")


class AggregateTask(luigi.Task):
    date = luigi.DateParameter()

    def run(self):
        with db_connect("resources/analytics.db") as conn:
            cursor = conn.cursor()

            sql = f"""INSERT INTO sales_report (date, total_sales)
             SELECT date_ordered as date, sum(order_value) as total_sales FROM analytics 
             WHERE date_ordered='{self.date.isoformat()}' GROUP BY date_ordered
             """
            cursor.execute(sql)

            with self.output().open("w") as t_file:
                t_file.write("SUCCESS")

            conn.commit()

    def requires(self):
        return LoadToAnalyticsDBTask(date=self.date)

    def output(self):
        return luigi.local_target.LocalTarget(f"target_files/report_{self.date.isoformat()}.txt")


class SalesReport(luigi.task.MixinNaiveBulkComplete, luigi.WrapperTask):
    date = luigi.DateParameter()

    def requires(self):

        yield UploadToGCSTask(date=self.date)

        yield AggregateTask(date=self.date)
