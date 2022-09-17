first_name_query = "select * from df_table where first_name = 'Donald' " \
                   "and DOJ = mdy(current_date)"
# mdy("08-30-2022") --> 30Aug2022

Second_name_query = "select * from Employee where EMPLOYEE_ID > 200"