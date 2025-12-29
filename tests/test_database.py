import unittest
from src.helpers.database import *

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

        self.assertEqual(len(rows[0]),4)
        self.assertIsInstance(rows[0][0],int)

    def test_insert(self):
        insert_into_raw(1,"2025-11-20T21:00:00", 20.0, 40.0, 500, self.conn)
        self.cur.execute("""SELECT * FROM public."Raw_readings" WHERE "Sensor_id"=1 AND "Co2"=500""")
        response=self.cur.fetchall()

        self.assertEqual(len(response[0]),5)
        self.assertEqual(str(response[0]), "(1, datetime.datetime(2025, 11, 20, 21, 0), Decimal('20.0'), Decimal('40.0'), 500)")

        insert_into_aggregated(1,"2025-11-20T21:00:00", 20.0, 40.0, 500, self.conn)

        self.cur.execute("""SELECT * FROM public."Aggregated_metrics" WHERE "Sensor_id"=1 AND "avg_Co2"=500""")
        response=self.cur.fetchall()

        self.assertEqual(len(response[0]),5)
        self.assertEqual(str(response[0]), "(1, datetime.datetime(2025, 11, 20, 21, 0), Decimal('20.0'), Decimal('40.0'), 500)")

        insert_into_alerts(1,"2025-11-20T21:00:00",'c',"TEST", self.conn)

        self.cur.execute("""SELECT * FROM public."Alerts" WHERE "Sensor_id"=1 AND "TimeStamp"='2025-11-20T21:00:00'""")
        response=self.cur.fetchall()

        self.assertEqual(len(response[0]),4)
        self.assertEqual(str(response[0]), "(1, datetime.datetime(2025, 11, 20, 21, 0), 'c', 'TEST')")



    @classmethod
    def tearDownClass(cls):
        cls.cur.execute("""DELETE FROM public."Raw_readings" WHERE "Sensor_id"=1 AND "TimeStamp" ='2025-11-20T21:00:00' AND "Temperature"=20.0 AND "Humidity"=40.0 AND "Co2"=500; """)
        cls.cur.execute("""DELETE FROM public."Aggregated_metrics" WHERE "Sensor_id"=1 AND "Window_start" ='2025-11-20T21:00:00' AND "avg_Temp"=20.0 AND "avg_Humidity"=40.0 AND "avg_Co2"=500; """)
        cls.cur.execute("""DELETE FROM public."Alerts" WHERE "Sensor_id"=1 AND "TimeStamp"='2025-11-20T21:00:00' AND "error_Type"='c' AND "Problem_message"= 'TEST'; """)
        cls.conn.commit()
        cls.cur.close()
        cls.conn.close()



if __name__ == '__main__':
    unittest.main() 