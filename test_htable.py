from helper import get_phi_args, get_db_connection
from mf_processor import MFStructure
import pprint

def test_h_table_creation():
    # Get Phi arguments
    print("Reading Phi arguments...")
    phi = get_phi_args()
    if not phi:
        print("Failed to get Phi arguments!")
        return

    # Create MF Structure
    print("\nInitializing MF Structure...")
    mf = MFStructure(
        select_attrs=','.join(phi.select_attrs),
        grouping_vars=str(phi.num_grouping_vars),
        grouping_attrs=','.join(phi.grouping_attrs),
        f_vect=','.join(phi.f_vect),
        conditions=';'.join(phi.conditions),
        having=phi.having
    )

    # Connect to database
    print("\nConnecting to database...")
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to database!")
        return

    cur = conn.cursor()
    
    # Process first scan to populate H table structure
    print("\nProcessing first scan to create H table entries...")
    cur.execute("SELECT * FROM sales")
    scan_number = 0
    processed_customers = set()
    
    for row in cur:
        # Convert row to dictionary for easier access
        row_dict = dict(row)
        cust = row_dict['cust']
        
        # Only process each customer once for initial H table creation
        if cust not in processed_customers:
            mf.process_tuple(row_dict, scan_number)
            processed_customers.add(cust)
    
    # Print H table structure
    print("\nInitialized H Table Structure:")
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(mf.mf_struct)
    
    # Print some statistics
    print(f"\nTotal number of groups (customers): {len(mf.mf_struct)}")
    
    # Show structure for first customer
    if mf.mf_struct:
        first_cust = next(iter(mf.mf_struct))
        print(f"\nDetailed structure for first customer ({first_cust}):")
        pp.pprint(mf.mf_struct[first_cust])

    conn.close()

if __name__ == "__main__":
    test_h_table_creation()