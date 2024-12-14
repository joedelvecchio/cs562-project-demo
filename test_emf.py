from helper import get_phi_args, get_db_connection
from mf_processor import MFStructure
import pprint

def test_emf_processing():
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
    
    # Process all scans
    results = mf.process_all_scans(cur)
    
    # Print results
    print("\nFinal Results:")
    # Dynamic column headers based on select attributes
    headers = [attr for attr in phi.select_attrs]
    
    # Print table header
    print(" | ".join(f"{header:<10}" for header in headers))
    print("-" * (len(headers) * 12))
    
    # Print rows
    for row in results:
        print(" | ".join(f"{str(row.get(header, '')):<10}" for header in headers))

    conn.close()

if __name__ == "__main__":
    test_emf_processing()