import psycopg2

#reading from file
def read_file(file):
    try:
        # Read file contents
        with open(file_path, 'r') as f:
            contents = f.read().splitlines()

        # Parse the contents
        select = ""
        grouping_variables = ""
        grouping_attributes = ""
        list_of_aggregates = ""
        select_condition = ""
        having_clause = ""
        idx = 0
        while idx < len(contents):
            line = contents[idx]
            if line == 'SELECT ATTRIBUTE(S):':
                select = lines[x+1].replace(" ", "") 
                x += 2
            elif line == 'NUMBER OF GROUPING VARIABLES(n):':
                grouping_variables = lines[x+1].replace(" ", "")
                x += 2
            elif line == 'GROUPING ATTRIBUTES(V):':
                grouping_attributes = lines[x+1].replace(" ", "")
                x += 2
            elif line == 'F-VECT([F]):':
                list_of_aggregates = lines[x+1].replace(" ", "")
                x += 2
            elif line == 'SELECT CONDITION-VECT([C]):':
                select_condition = lines[x+1].replace(" ", "")
                x += 2
            elif line == 'HAVING CLAUSE (G):':
                having_clause = lines[x+1]
                x += 2
    except:
        print("Error processing file")
        read_file(file)

        
#manual input from user
def user_input():
    select = input("Enter the SELECT clause: ")
    grouping_variables = input("Enter the number of grouping variables: ")
    grouping_attributes = input("Enter the grouping attributes: ")
    list_of_aggregates = input("Enter the list of aggregate functions: ")
    select_condition = input("Enter the predicates for the grouping variables: ")
    having_clause = input("Enter the HAVING clause: ")

#if user wants to read from file or provide input
def get_phi_args(file):
    print("Select one of the options when prompted: ")
    print("Enter '1' to read from a file")
    print("Enter '2' to input manually")
    choice = input("Enter your choice: ")
    if choice == "1":
        read_file(file)
        return
    elif choice == "2":
        user_input()
        return
    else:
        print("Invalid input. Select a valid option.")
        get_phi_args()

#connect to database 
def db_connect():
        db = psycopg2.connect(
        host = '',
        dbname = '',
        user = '',
        password = '',
        port = 
    )
    cursor = db.cursor()
    return cursor, db

#initialize the database
def db_init(db):
    init = open("load_sales_10000_table (NEW).sql", "r")
    for line in init:
        db.execute(line)
    init.close()
    
#generate code for H table 
