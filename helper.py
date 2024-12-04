def create_mf_struct(file):
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

def print_mf_struct(mf_struct):
    for key, value in mf_struct.items():
        print(key)
        print(value, end='\n')
