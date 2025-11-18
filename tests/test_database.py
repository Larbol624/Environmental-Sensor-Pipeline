import unittest
from src.database import get_connection, get_all_raw, get_all_aggregated, get_all_alerts,insert_into_raw, insert_into_aggregated, insert_into_alerts

class TestDatabase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.conn= get_connection()
        cls.cur=cls.conn.cursor()
         
    def test_select(self):
        rows=get_all_raw(self.conn)
    
        self.assertEqual(len(rows[0]),5)
        self.assertIsInstance(rows[0][0],int)

        rows=get_all_aggregated(self.conn)

        self.assertEqual(len(rows[0]),5)
        self.assertIsInstance(rows[0][0],int)

        rows=get_all_alerts(self.conn)

        self.assertEqual(len(rows[0]),3)
        self.assertIsInstance(rows[0][0],int)

    def test_insert(self):
        insert_into_raw(1,"21:00:00", 20.0, 40.0, 500, self.conn)
        self.cur.execute("""SELECT * FROM public."Raw_readings" WHERE "Sensor_id"=1 AND "Co2"=500""")
        response=self.cur.fetchall()

        self.assertEqual(len(response[0]),5)
        self.assertEqual(str(response[0]), "(1, datetime.time(21, 0), Decimal('20.0'), Decimal('40.0'), 500)")

        insert_into_aggregated(1,"21:00:00", 20.0, 40.0, 500, self.conn)

        self.cur.execute("""SELECT * FROM public."aggregated_metrics" WHERE "sensor_id"=1 AND "avg_co2"=500""")
        response=self.cur.fetchall()

        self.assertEqual(len(response[0]),5)
        self.assertEqual(str(response[0]), "(1, datetime.time(21, 0), Decimal('20.0'), Decimal('40.0'), 500)")

        insert_into_alerts(1,"21:00:00",'c',self.conn)

        self.cur.execute("""SELECT * FROM public."alerts" WHERE "sensor_id"=1 AND "timestamp"='21:00:00'""")
        response=self.cur.fetchall()

        self.assertEqual(len(response[0]),3)
        self.assertEqual(str(response[0]), "(1, datetime.time(21, 0), 'c')")



    @classmethod
    def tearDownClass(cls):
        cls.cur.close()
        cls.conn.close()


if __name__ == '__main__':
    unittest.main() 