import sqlite3 as sql
import socket
import os

import pandas as pd


_DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "timing_data.db"))


def _get_purified_hostname():
    """Convert hostname into standard internal notation.

    Converts the resulting hostname into internal standard: all
    lowercase, with no numerics, so e.g. 'rzAnsel194' becomes 'rzansel'
    """
    return "".join(char for char in socket.gethostname() if not char.isdigit()).lower()



def save_timing_data(jobcount, seconds, implementation):
    """Save timing data to a SQLite table."""
    db_handle = sql.connect(_DB_PATH)
    _create_table(db_handle)
    with db_handle:
        db_handle.execute(
            "INSERT INTO Timings (jobcount, timing, implementation, hostname) "
            "VALUES (?, ?, ?, ?)",
            (jobcount, seconds, implementation, _get_purified_hostname())
        )


def fetch_timing_data():
    """Save timing data to a SQLite table."""
    db_handle = sql.connect(_DB_PATH)
    db_handle.row_factory = sql.Row
    with db_handle:
        cursor = db_handle.execute("SELECT * FROM Timings ORDER BY jobcount ASC, implementation ASC")
        return pd.DataFrame(dict(row) for row in cursor.fetchall())


def _create_table(db_handle):
    """Create a SQLite table"""
    with db_handle:
        db_handle.execute(
            "CREATE TABLE IF NOT EXISTS Timings "
            "(jobcount INTEGER, timing REAL, implementation TEXT, "
            "hostname TEXT, timestamp TEXT DEFAULT CURRENT_TIMESTAMP)"
        )

def analyse(df):
    df['jobs_per_second'] = df['jobcount'] / df['timing']
    print(df.groupby('implementation').mean())[["jobs_per_second"]]


if __name__ == '__main__':
    analyse(fetch_timing_data())
