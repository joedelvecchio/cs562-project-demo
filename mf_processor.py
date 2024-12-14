class MFStructure:
    def __init__(self, select_attrs, grouping_vars, grouping_attrs, f_vect, conditions, having):
        """
        Initialize MF Structure with Phi operation parameters
        
        Parameters:
        - select_attrs: comma-separated string of attributes to select
        - grouping_vars: number of grouping variables
        - grouping_attrs: comma-separated string of grouping attributes
        - f_vect: comma-separated string of aggregate functions
        - conditions: semicolon-separated string of conditions for each grouping variable
        - having: having clause condition (or '-' if none)
        """
        self.select_attrs = [attr.strip() for attr in select_attrs.split(',')]
        self.num_grouping_vars = int(grouping_vars)
        self.grouping_attrs = [attr.strip() for attr in grouping_attrs.split(',')]
        self.f_vect = [f.strip() for f in f_vect.split(',')]
        self.conditions = [cond.strip() for cond in conditions.split(';')]
        self.having = having
        self.mf_struct = {}
        
        # Map of column names to their PostgreSQL data types
        self.schema = {
            'cust': 'varchar',
            'prod': 'varchar',
            'day': 'integer',
            'month': 'integer',
            'year': 'integer',
            'state': 'char',
            'quant': 'integer',
            'date': 'date'
        }
    
    def process_tuple(self, tuple_data, scan_number):
        """
        Process a single tuple for a specific scan number
        
        Parameters:
        - tuple_data: Dictionary containing a row from the sales table
        - scan_number: Current scan number (0-based index)
        """
        # Debug print
        print(f"\nProcessing tuple for scan {scan_number}")
        print(f"Condition for this scan: {self.conditions[scan_number]}")
        print(f"Tuple data: {tuple_data}")
        
        # Get grouping key (e.g., customer for grouping by customer)
        group_key = tuple_data[self.grouping_attrs[0]]  # Assuming single grouping attribute for now
        
        # If this is first scan (scan_number == 0), initialize entry
        if scan_number == 0:
            if group_key not in self.mf_struct:
                # Initialize the entry
                self.mf_struct[group_key] = self._initialize_aggregates()
                # Store the actual grouping attribute value
                self.mf_struct[group_key][self.grouping_attrs[0]] = group_key
        
        # Check condition
        condition_result = self._check_condition(tuple_data, self.conditions[scan_number])
        print(f"Condition check result: {condition_result}")
        
        # Continue with existing logic if condition is true
        if condition_result:
            self._update_aggregates(group_key, tuple_data, scan_number)
    
    def _initialize_aggregates(self):
        """Initialize aggregate values for a new group"""
        aggregates = {}
        # Keep track of the original grouping attribute
        for attr in self.grouping_attrs:
            aggregates[attr] = None
            
        # Initialize aggregates based on f_vect from first input
        # e.g., count_1_quant, sum_2_quant, max_3_quant
        for f in self.f_vect:
            if 'count_' in f:
                aggregates[f] = 0
            elif 'sum_' in f:
                aggregates[f] = 0
            elif 'max_' in f:
                aggregates[f] = float('-inf')
            elif 'min_' in f:
                aggregates[f] = float('inf')
            elif 'avg_' in f:
                # For average, store sum and count separately
                aggregates[f + '_sum'] = 0
                aggregates[f + '_count'] = 0

        return aggregates
    
    def _check_condition(self, tuple_data, condition):
        """
        Check if tuple satisfies the condition for current scan
        
        Parameters:
        - tuple_data: Dictionary containing a row from the sales table
        - condition: Condition string (e.g., "state='NY' and year=2023")
        """
        if not condition or condition == '-':
            return True
                
        try:
            # Handle multiple conditions joined by 'and'
            sub_conditions = [cond.strip() for cond in condition.split(' and ')]
            
            for sub_condition in sub_conditions:
                # Find the comparison operator
                operators = ['>=', '<=', '!=', '=', '>', '<']
                found_op = False
                
                for op in operators:
                    if op in sub_condition:
                        parts = sub_condition.split(op)
                        if len(parts) != 2:
                            print(f"Invalid condition format: {sub_condition}")
                            return False
                        
                        field_expr, value_expr = parts
                        
                        # Extract field name (remove grouping variable prefix if present)
                        field = field_expr.split('.')[-1].strip()
                        value = value_expr.strip().strip("'")
                        
                        # Get actual values
                        tuple_value = str(tuple_data[field]).strip()
                        
                        # Compare based on data type
                        if self.schema[field] in ('varchar', 'char'):
                            tuple_value = tuple_value.strip()
                            value = value.strip()
                            if not self._compare_values(tuple_value, value, op):
                                return False
                        elif self.schema[field] == 'integer':
                            if not self._compare_values(int(tuple_value), int(value), op):
                                return False
                        elif self.schema[field] == 'date':
                            if not self._compare_values(tuple_value, value, op):
                                return False
                        
                        found_op = True
                        break
                
                if not found_op:
                    print(f"No valid operator found in condition: {sub_condition}")
                    return False
            
            return True
            
        except Exception as e:
            print(f"Error in condition check: {e}")
            return False
    
    def _compare_values(self, val1, val2, op):
        """Compare two values based on the operator"""
        if op == '=':
            return val1 == val2
        elif op == '!=':
            return val1 != val2
        elif op == '>':
            return val1 > val2
        elif op == '<':
            return val1 < val2
        elif op == '>=':
            return val1 >= val2
        elif op == '<=':
            return val1 <= val2
        return False
    
    def _update_aggregates(self, group_key, tuple_data, scan_number):
        """Update aggregate values for a group based on current tuple"""
        entry = self.mf_struct[group_key]
        scan_prefix = f"{scan_number + 1}_"
        
        for f in self.f_vect:
            if not f.startswith(scan_prefix):
                continue
                
            agg_parts = f.split('_')
            if len(agg_parts) >= 3:
                agg_type = agg_parts[1]
                field = '_'.join(agg_parts[2:])
                
                if agg_type == 'sum':
                    entry[f] += tuple_data[field]
                elif agg_type == 'count':
                    entry[f] += 1
                elif agg_type == 'max':
                    entry[f] = max(entry[f], tuple_data[field])
                elif agg_type == 'min':
                    entry[f] = min(entry[f], tuple_data[field])
                elif agg_type == 'avg':
                    entry[f + '_sum'] += tuple_data[field]
                    entry[f + '_count'] += 1
    
    def evaluate_having(self):
        """Apply having clause to filter results"""
        if not self.having or self.having == '-':
            return
            
        filtered_struct = {}
        for group_key, entry in self.mf_struct.items():
            # Compute averages before evaluation
            for f in self.f_vect:
                if '_avg_' in f:
                    if entry[f + '_count'] > 0:
                        entry[f] = entry[f + '_sum'] / entry[f + '_count']
                    else:
                        entry[f] = 0
                        
            # Evaluate having condition
            if self._evaluate_having_condition(entry):
                filtered_struct[group_key] = entry
                
        self.mf_struct = filtered_struct
    
    def _evaluate_having_condition(self, entry):
        """Evaluate having condition for a group"""
        if not self.having or self.having == '-':
            return True
            
        try:
            # Import math to use math.inf
            import math
            
            # Replace aggregate function references with actual values
            condition = self.having
            for f in self.f_vect:
                if f in condition:
                    if '_avg_' in f:
                        if entry.get(f + '_count', 0) > 0:
                            value = entry[f + '_sum'] / entry[f + '_count']
                        else:
                            value = 0
                    else:
                        value = entry.get(f, 0)
                    condition = condition.replace(f, str(value))
            
            return eval(condition)
        except Exception as e:
            print(f"Error in having condition evaluation: {e}")
            return True
    
    def get_results(self):
        """Get final results in tabular format"""
        results = []
        for group_key, entry in self.mf_struct.items():
            row = {}
            # Add grouping attributes
            for attr in self.grouping_attrs:
                row[attr] = entry.get(attr, None)
            
            # Add aggregate values
            for attr in self.select_attrs:
                if attr in self.grouping_attrs:
                    continue
                if '_avg_' in attr:
                    count_key = attr + '_count'
                    sum_key = attr + '_sum'
                    if entry.get(count_key, 0) > 0:
                        row[attr] = round(entry[sum_key] / entry[count_key], 2)
                    else:
                        row[attr] = 0
                else:
                    row[attr] = entry.get(attr, 0)
            results.append(row)
        
        # Sort results by grouping attributes
        return sorted(results, key=lambda x: [str(x.get(attr, '')) for attr in self.grouping_attrs])

    def process_all_scans(self, cursor):
        """Process all scans according to EMF algorithm 3.1"""
        print("\nProcessing all scans...")
        
        for scan in range(self.num_grouping_vars):
            print(f"\nStarting scan {scan + 1} with condition: {self.conditions[scan]}")
            cursor.execute("SELECT * FROM sales")
            count_total = 0
            count_matched = 0
            for row in cursor:
                count_total += 1
                if self._check_condition(dict(row), self.conditions[scan]):
                    self.process_tuple(dict(row), scan)
                    count_matched += 1
            print(f"Total rows: {count_total}, Matched rows: {count_matched}")
        
        print("\nApplying having clause...")
        self.evaluate_having()
        
        print("\nGenerating final results...")
        return self.get_results()