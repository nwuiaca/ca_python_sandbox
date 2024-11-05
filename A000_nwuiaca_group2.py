"""
SCHEDULER GROUP 2 FUNCTIONS
Auther: Albert Janse van Rensburg (NWU:21162395)
Create date: 25 Sep 2023
"""

# Import own functions
from _my_modules import funcsys
import A003_table_vacuum


def group2_functions():
    """
    Script to run workday noon functions
    :return: Nothing
    """

    # Declare variables

    # Vacuum tables
    try:
        A003_table_vacuum.table_vacuum()
    except Exception as e:
        funcsys.ErrMessage(e)


if __name__ == '__main__':
    group2_functions()
