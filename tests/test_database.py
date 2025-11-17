import unittest
from src.database import get_connection, get_all_raw


class TestDatabase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn= get_connection()
        cls.cur=cls.conn.cursor()
         
    def test_select(self):
        rows=get_all_raw(self.conn)
    
        self.assertEqual(len(rows[0]),5)
        self.assertIsInstance(rows[0][0],int)

    @classmethod
    def tearDown(cls):
        cls.cur.close()
        cls.conn.close()


if __name__ == '__main__':
    unittest.main() 