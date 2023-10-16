import mysql.connector

class BiographyReader:
    def __init__(self, host, user, password, database):
        self.conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.conn.cursor()

    def get_biography(self, name):
        query = "SELECT biography FROM Biography WHERE name = %s"
        self.cursor.execute(query, (name,))
        result = self.cursor.fetchone()

        if result:
            return result[0]
        else:
            return f"No biography found for {name}"

    def close_connection(self):
        self.conn.close()
