import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import os

class PhiOperator:
    """Class to store and validate Phi operator arguments"""
    def __init__(self):
        self.select_attrs = None        # S: List of projected attributes
        self.num_grouping_vars = None             # n: Number of grouping variables
        self.grouping_attrs = None     # V: List of grouping attributes
        self.f_vect = None            # F: List of aggregate functions
        self.conditions = None        # σ: List of predicates/conditions
        self.having = None           # G: Having clause predicate
    
    def is_valid(self):
        """Validate that all required arguments are set"""
        return (self.select_attrs and 
            self.num_grouping_vars is not None and 
            self.grouping_attrs and 
            self.f_vect and 
            self.conditions is not None)

def read_phi_from_file(file_path):
    """Read Phi operator arguments from input file"""
    phi = PhiOperator()
    
    try:
        with open(file_path, 'r') as f:
            lines = f.read().splitlines()
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            if line == 'SELECT ATTRIBUTE(S):':
                phi.select_attrs = [attr.strip() for attr in lines[i+1].split(',')]
                i += 2
            elif line == 'NUMBER OF GROUPING VARIABLES(n):':
                phi.num_grouping_vars = int(lines[i+1].strip())
                i += 2
            elif line == 'GROUPING ATTRIBUTES(V):':
                phi.grouping_attrs = [attr.strip() for attr in lines[i+1].split(',')]
                i += 2
            elif line == 'F-VECT([F]):':
                phi.f_vect = [f.strip() for f in lines[i+1].split(',')]
                i += 2
            elif line == 'SELECT CONDITION-VECT([C]):':
                phi.conditions = [cond.strip() for cond in lines[i+1].split(';')]
                i += 2
            elif line == 'HAVING CLAUSE (G):':
                phi.having = lines[i+1].strip()
                i += 2
            else:
                i += 1
    
        return phi if phi.is_valid() else None
    
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None

def get_phi_from_user():
    """Get Phi operator arguments from user input"""
    phi = PhiOperator()
    
    try:
        print("\nEnter Phi operator arguments:")
        
        attrs = input("Enter SELECT attributes (comma-separated): ")
        phi.select_attrs = [attr.strip() for attr in attrs.split(',')]
        
        phi.num_grouping_vars = int(input("Enter number of grouping variables: "))
        
        g_attrs = input("Enter grouping attributes (comma-separated): ")
        phi.grouping_attrs = [attr.strip() for attr in g_attrs.split(',')]
        
        f_vect = input("Enter aggregate functions (comma-separated): ")
        phi.f_vect = [f.strip() for f in f_vect.split(',')]
        
        print("\nEnter conditions for each grouping variable")
        print("(one per line, press Enter twice when done):")
        conditions = []
        while True:
            cond = input().strip()
            if not cond:
                break
            conditions.append(cond)
        phi.conditions = conditions
        
        phi.having = input("Enter HAVING clause (or '-' for none): ")
        
        return phi if phi.is_valid() else None
    
    except Exception as e:
        print(f"Error getting user input: {e}")
        return None

def get_phi_args():
    """Main function to get Phi operator arguments"""
    print("How would you like to provide the Phi operator arguments?")
    print("1. Read from file")
    print("2. Manual input")
    
    choice = input("\nEnter your choice (1 or 2): ")
    
    if choice == '1':
        file_path = input("Enter the input file path: ")
        phi = read_phi_from_file(file_path)
    elif choice == '2':
        phi = get_phi_from_user()
    else:
        print("Invalid choice!")
        return None
    
    if phi is None:
        print("Failed to get valid Phi operator arguments!")
        return None
        
    return phi

def get_db_connection():
    """Establish and return a database connection"""
    try:
        load_dotenv()
        
        # Get database credentials from environment variables
        user = os.getenv('USER')
        password = os.getenv('PASSWORD')
        dbname = os.getenv('DBNAME')
        
        if not all([user, password, dbname]):
            raise ValueError("Missing database credentials in .env file")
        
        # Connect to the database
        conn = psycopg2.connect(
            f"dbname={dbname} user={user} password={password}",
            cursor_factory=psycopg2.extras.DictCursor
        )
        return conn
        
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

# For testing
if __name__ == "__main__":
    # Test Phi argument parsing
    print("Testing Phi argument parsing:")
    phi = get_phi_args()
    if phi:
        print("\nPhi Operator Arguments:")
        print(f"Select Attributes: {phi.select_attrs}")
        print(f"Number of Grouping Variables: {phi.num_grouping_vars}")
        print(f"Grouping Attributes: {phi.grouping_attrs}")
        print(f"Aggregate Functions: {phi.f_vect}")
        print(f"Conditions: {phi.conditions}")
        print(f"Having Clause: {phi.having}")
    
    # Test database connection
    print("\nTesting database connection:")
    conn = get_db_connection()
    if conn:
        print("Successfully connected to database")
        conn.close()
