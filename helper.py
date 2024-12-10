#reading from file
def read_file(file):
    mf_struct = {}
    try:
        # Read file contents
        with open(file_path, 'r') as f:
            contents = f.read().splitlines()

        # Parse the contents
        idx = 0
        while idx < len(contents):
            line = contents[idx]

            if line == 'SELECT ATTRIBUTE(S):':
                mf_struct['select'] = contents[idx + 1].strip()
                idx += 1
            elif line == 'NUMBER OF GROUPING VARIABLES(n):':
                mf_struct['groupingVariables'] = int(contents[idx + 1].strip())
                idx += 1
            elif line == 'GROUPING ATTRIBUTES(V):':
                mf_struct['groupingAttributes'] = contents[idx + 1].strip()
                idx += 1
            elif line == 'F-VECT([F]):':
                mf_struct['listOfAggregateFuncs'] = [agg.strip().lower() for agg in contents[idx + 1].split(',')]
                idx += 1
            elif line == 'SELECT CONDITION-VECT([C]):':
                conditions = []
                idx += 1
                while idx < len(contents) and contents[idx] != 'HAVING CLAUSE (G):':
                    conditions.append(contents[idx].strip())
                    idx += 1
                mf_struct['selectConditionVector'] = conditions
                idx -= 1  # Adjust index since HAVING CLAUSE will also be processed
            elif line == 'HAVING CLAUSE (G):':
                mf_struct['havingClause'] = contents[idx + 1].strip()
                idx += 1

            idx += 1

    except (IOError, ValueError, IndexError) as e:
        print(f"Error processing file: {e}")
        return None

    return mf_struct
        
#manual input from user
def user_input():
    select = input("Enter the SELECT clause: ")
    grouping_variables = input("Enter the number of grouping variables: ")
    grouping_attributes = input("Enter the grouping attributes: ")
    list_of_aggregates = input("Enter the list of aggregate functions: ")
    select_condition = input("Enter the predicates for the grouping variables: ")
    having_clause = input("Enter the HAVING clause: ")

#if user wants to read from file or provide input
def get_phi_args():
    print("Select one of the options when prompted: ")
    print("Enter '1' to read from a file")
    print("Enter '2' to input manually")
    choice = input("Enter your choice: ")
    if choice == "1":
        read_file()
        return
    elif choice == "2":
        user_input()
        return
    else:
        print("Invalid input. Select a valid option.")
        get_phi_args()

#connect to database 
def db_connect():

#initialize the database
def db_init():

#generate code for H table 
