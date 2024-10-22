import pandas as pd
import time

from proton_driver import client

if __name__ == "__main__":
    c = client.Client(host='127.0.0.1', port=8463)

    # setup the test stream
    c.execute("drop stream if exists test")
    c.execute(
        """create stream test (
                    year int16,
                    first_name string
                )"""
    )
    # add some data
    df = pd.DataFrame.from_records(
        [
            {'year': 1994, 'first_name': 'Vova'},
            {'year': 1995, 'first_name': 'Anja'},
            {'year': 1996, 'first_name': 'Vasja'},
            {'year': 1997, 'first_name': 'Petja'},
        ]
    )
    c.insert_dataframe(
        'INSERT INTO "test" (year, first_name) VALUES',
        df,
        settings=dict(use_numpy=True),
    )
    # or c.execute(
    #     "INSERT INTO test(year, first_name) VALUES", df.to_dict('records')
    # )
    # wait for 3 sec to make sure data available in historical store
    time.sleep(3)

    df = c.query_dataframe('SELECT * FROM table(test)')
    print(df)
    print(df.describe())
