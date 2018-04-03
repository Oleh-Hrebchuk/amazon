import sqlite3


def create_table():
    conn = sqlite3.connect('amazon.sqlite3')
    cursor = conn.cursor()

    create_cost = """
    CREATE TABLE IF NOT EXISTS cost (
    object_type text NOT NULL,
    object_id VARCHAR(32) NOT NULL,
    cost float NOT NULL);
    """

    # set index as object_type is criteria of record uniqueness
    create_index = """
    CREATE INDEX IF NOT EXISTS cost_object_type_idx ON cost (object_type);
    """

    cursor.execute(create_cost)
    cursor.execute(create_index)
    cursor.close()

if __name__ == "__main__":
    # Create database and table.
    create_table()

