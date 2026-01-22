import psycopg2


class UserCounterService:
    def __init__(self, database_url):
        self.database_url = database_url

    def _get_connection(self, serializable=False):
        conn = psycopg2.connect(self.database_url)
        if serializable:
            conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
            )
        return conn

    def setup(self):
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_counter (
                user_id INT PRIMARY KEY,
                counter INT,
                version INT
            )
        """)

        cursor.execute("""
            INSERT INTO user_counter (user_id, counter, version)
            VALUES (1, 0, 0)
            ON CONFLICT (user_id)
            DO UPDATE SET counter = 0, version = 0
        """)

        conn.commit()
        cursor.close()
        conn.close()
        print("Database setup complete")


    def increment_lost_update(self, thread_id, n=10_000):
        conn = self._get_connection()
        cursor = conn.cursor()

        for _ in range(n):
            cursor.execute(
                "SELECT counter FROM user_counter WHERE user_id = 1"
            )
            counter = cursor.fetchone()[0] + 1

            cursor.execute(
                "UPDATE user_counter SET counter = %s WHERE user_id = %s",
                (counter, 1)
            )
            conn.commit()

        cursor.close()
        conn.close()

    def increment_serializable(self, thread_id, n=10_000):
        conn = self._get_connection(serializable=True)
        cursor = conn.cursor()

        successful = 0

        while successful < n:
            try:
                cursor.execute(
                    "SELECT counter FROM user_counter WHERE user_id = 1"
                )
                counter = cursor.fetchone()[0] + 1

                cursor.execute(
                    "UPDATE user_counter SET counter = %s WHERE user_id = %s",
                    (counter, 1)
                )
                conn.commit()
                successful += 1

            except psycopg2.errors.SerializationFailure:
                conn.rollback()

        cursor.close()
        conn.close()

    def increment_in_place(self, thread_id, n=10_000):
        conn = self._get_connection()
        cursor = conn.cursor()

        for _ in range(n):
            cursor.execute(
                "UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s",
                (1,)
            )
            conn.commit()

        cursor.close()
        conn.close()


    def increment_row_locking(self, thread_id, n=10_000):
        conn = self._get_connection()
        cursor = conn.cursor()

        for _ in range(n):
            cursor.execute(
                "SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE"
            )
            counter = cursor.fetchone()[0] + 1

            cursor.execute(
                "UPDATE user_counter SET counter = %s WHERE user_id = %s",
                (counter, 1)
            )
            conn.commit()

        cursor.close()
        conn.close()


    def increment_optimistic(self, thread_id, n=10_000):
        conn = self._get_connection()
        cursor = conn.cursor()

        for _ in range(n):
            while True:
                cursor.execute(
                    "SELECT counter, version FROM user_counter WHERE user_id = 1"
                )
                row = cursor.fetchone()
                counter, version = row

                cursor.execute(
                    "UPDATE user_counter SET counter = %s, version = %s WHERE user_id = %s AND version = %s",
                    (counter + 1, version + 1, 1, version)
                )
                conn.commit()

                if cursor.rowcount > 0:
                    break

        cursor.close()
        conn.close()


    def get_counter(self, user_id=1):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT counter FROM user_counter WHERE user_id = %s",
            (user_id,)
        )
        value = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return value
