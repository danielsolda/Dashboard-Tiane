import sqlite3
import time
import os

DB_PATH = os.environ.get('DB_PATH', 'kommo_tokens.db')


class TokenStorage:
    def __init__(self, db_path=None):
        self.db_path = db_path or DB_PATH
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS tokens (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    access_token TEXT NOT NULL,
                    refresh_token TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    subdomain TEXT NOT NULL
                )
            ''')
            conn.commit()

    def save_token(self, access_token, refresh_token, expires_in, subdomain):
        expires_at = time.time() + expires_in
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO tokens (id, access_token, refresh_token, expires_at, subdomain)
                VALUES (1, ?, ?, ?, ?)
            ''', (access_token, refresh_token, expires_at, subdomain))
            conn.commit()

    def get_token(self):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute('SELECT access_token, refresh_token, expires_at, subdomain FROM tokens WHERE id = 1').fetchone()
        if row:
            return {
                'access_token': row[0],
                'refresh_token': row[1],
                'expires_at': row[2],
                'subdomain': row[3],
            }
        return None

    def is_expired(self):
        token = self.get_token()
        if not token:
            return True
        return time.time() >= token['expires_at'] - 60  # 60s margin

    def delete_token(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('DELETE FROM tokens WHERE id = 1')
            conn.commit()
