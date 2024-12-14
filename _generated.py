import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv
from mf_processor import MFStructure

def query():
    """
    Execute the EMF query using MFStructure.
    Returns formatted results table.
    """
    try:
        # Database connection setup
        load_dotenv()
        user = os.getenv('USER')
        password = os.getenv('PASSWORD')
        dbname = os.getenv('DBNAME')

        conn = psycopg2.connect(
            f"dbname={dbname} user={user} password={password}",
            cursor_factory=psycopg2.extras.DictCursor
        )
        cur = conn.cursor()
        
        # Initialize MF Structure with Phi operator arguments
        mf = MFStructure(
            select_attrs="cust,count_1_quant,sum_2_quant,max_3_quant",
            grouping_vars="3",
            grouping_attrs="cust",
            f_vect="count_1_quant,sum_2_quant,max_3_quant",
            conditions="1.state = 'NY';2.state = 'NJ';3.state = 'CT'",
            having="-"
        )
        
        # Process EMF query using algorithm 3.1
        print("\nExecuting EMF query...")
        print("Original query specifications:")
        print("- Select attributes: cust, count_1_quant, sum_2_quant, max_3_quant")
        print("- Number of grouping variables: 3")
        print("- Grouping attributes: cust")
        print("- Aggregate functions: count_1_quant, sum_2_quant, max_3_quant")
        print("- Conditions: 1.state = 'NY'; 2.state = 'NJ'; 3.state = 'CT'")
        if "-" != "-":
            print("- Having clause: -")
        
        # Execute query and get results
        results = mf.process_all_scans(cur)
        
        conn.close()
        
        # Format and return results
        if not results:
            return "No results found."
        return tabulate.tabulate(results, headers="keys", tablefmt="psql")
        
    except Exception as ex:
        return f"Error executing query: {str(ex)}"

def main():
    """Main function to execute query and display results"""
    print(query())

if __name__ == "__main__":
    main()