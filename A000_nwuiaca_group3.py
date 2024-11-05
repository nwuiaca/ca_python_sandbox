"""
SCHEDULER GROUP 3 FUNCTIONS
Auther: Albert Janse van Rensburg (NWU:21162395)
Create date: 25 Sep 2023
"""

# Import own functions
from _my_modules import funcsys
import A003_table_vacuum


def group3_functions():
    """
    Script to run workday afternoon functions
    :return: Nothing
    """

    # Declare variables

    # Vacuum tables
    s_function: str = 'A003_table_vacuum.table_vacuum'
    try:
        A003_table_vacuum.table_vacuum()
    except Exception as e:
        funcsys.ErrMessage(e)


if __name__ == '__main__':
    group3_functions()
