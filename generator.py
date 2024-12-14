import subprocess
from helper import get_phi_args

def generate_code(phi):
    """
    Generate code based on Phi operation arguments.
    The generated code will use MFStructure to process EMF queries.
    
    Parameters:
    - phi: PhiOperator object containing query parameters
    """
    
    # Generate complete program code
    program_code = f"""import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv
from mf_processor import MFStructure

def query():
    \"\"\"
    Execute the EMF query using MFStructure.
    Returns formatted results table.
    \"\"\"
    try:
        # Database connection setup
        load_dotenv()
        user = os.getenv('USER')
        password = os.getenv('PASSWORD')
        dbname = os.getenv('DBNAME')

        conn = psycopg2.connect(
            f"dbname={{dbname}} user={{user}} password={{password}}",
            cursor_factory=psycopg2.extras.DictCursor
        )
        cur = conn.cursor()
        
        # Initialize MF Structure with Phi operator arguments
        mf = MFStructure(
            select_attrs="{','.join(phi.select_attrs)}",
            grouping_vars="{phi.num_grouping_vars}",
            grouping_attrs="{','.join(phi.grouping_attrs)}",
            f_vect="{','.join(phi.f_vect)}",
            conditions="{';'.join(phi.conditions)}",
            having="{phi.having}"
        )
        
        # Process EMF query using algorithm 3.1
        print("\\nExecuting EMF query...")
        print("Original query specifications:")
        print("- Select attributes: {', '.join(phi.select_attrs)}")
        print("- Number of grouping variables: {phi.num_grouping_vars}")
        print("- Grouping attributes: {', '.join(phi.grouping_attrs)}")
        print("- Aggregate functions: {', '.join(phi.f_vect)}")
        print("- Conditions: {'; '.join(phi.conditions)}")
        if "{phi.having}" != "-":
            print("- Having clause: {phi.having}")
        
        # Execute query and get results
        results = mf.process_all_scans(cur)
        
        conn.close()
        
        # Format and return results
        if not results:
            return "No results found."
        return tabulate.tabulate(results, headers="keys", tablefmt="psql")
        
    except Exception as ex:
        return f"Error executing query: {{str(ex)}}"

def main():
    \"\"\"Main function to execute query and display results\"\"\"
    print(query())

if __name__ == "__main__":
    main()"""
    
    # Write generated program to file
    with open("_generated.py", "w") as f:
        f.write(program_code)
    
    return program_code

def main():
    """
    Main function to generate and execute query processing code.
    Gets Phi arguments and generates corresponding Python code.
    """
    # Get Phi arguments
    print("Getting Phi operator arguments...")
    phi = get_phi_args()
    
    if phi is None:
        print("Error: Failed to get Phi arguments")
        return
        
    try:
        # Generate query processing code
        print("\nGenerating query processing code...")
        generated_code = generate_code(phi)
        print("Code generation successful!")
        
        # Execute generated code
        print("\nExecuting generated code...")
        result = subprocess.run(["python", "_generated.py"], capture_output=True, text=True)
        
        if result.stderr:
            print("Errors during execution:")
            print(result.stderr)
        else:
            print(result.stdout)
            
    except Exception as e:
        print(f"Error during code generation or execution: {str(e)}")

if __name__ == "__main__":
    main()
