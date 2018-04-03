import sqlite3


if __name__ == "__main__":
    # Create database and table.
    conn = sqlite3.connect('amazon.sqlite3')
    cursor = conn.cursor()

    create_cost = """
    CREATE TABLE IF NOT EXISTS cost (
    object_type text NOT NULL,
    object_id VARCHAR(32) NOT NULL,
    cost float NOT NULL)
    """

    cursor.execute(create_cost)
    cursor.close()
