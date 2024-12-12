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
        # Create group key from grouping attributes
        group_key_parts = []
        for attr in self.grouping_attrs:
            val = tuple_data[attr]
            # Handle different data types
            if self.schema[attr] in ('varchar', 'char'):
                val = str(val).strip()
            group_key_parts.append(str(val))
        group_key = '_'.join(group_key_parts)
        
        # First scan: initialize entry if needed
        if scan_number == 0:
            if group_key not in self.mf_struct:
                self.mf_struct[group_key] = self._initialize_aggregates()
                # Store original values for grouping attributes
                for attr in self.grouping_attrs:
                    self.mf_struct[group_key][attr] = tuple_data[attr]
        
        # Check if tuple satisfies condition for current scan
        if self._check_condition(tuple_data, self.conditions[scan_number]):
            self._update_aggregates(group_key, tuple_data, scan_number)
    
    def _initialize_aggregates(self):
        """Initialize aggregate values for a new group"""
        aggregates = {}
        for f in self.f_vect:
            agg_parts = f.split('_')
            if len(agg_parts) >= 3:  # format: 1_sum_quant or similar
                scan_num, agg_type, field = agg_parts[0], agg_parts[1], '_'.join(agg_parts[2:])
                if agg_type == 'sum':
                    aggregates[f] = 0
                elif agg_type == 'count':
                    aggregates[f] = 0
                elif agg_type == 'max':
                    aggregates[f] = float('-inf')
                elif agg_type == 'min':
                    aggregates[f] = float('inf')
                elif agg_type == 'avg':
                    # For average, store sum and count separately
                    aggregates[f + '_sum'] = 0
                    aggregates[f + '_count'] = 0
        return aggregates
    
    def _check_condition(self, tuple_data, condition):
        """
        Check if tuple satisfies the condition for current scan
        
        Parameters:
        - tuple_data: Dictionary containing a row from the sales table
        - condition: Condition string (e.g., "state='NY'")
        """
        if not condition or condition == '-':
            return True
            
        # Parse condition
        try:
            # Handle different comparison operators
            for op in ['>=', '<=', '!=', '=', '>', '<']:
                if op in condition:
                    field_expr, value_expr = condition.split(op)
                    break
            
            # Extract field name (remove grouping variable prefix if present)
            field = field_expr.split('.')[-1].strip()
            value = value_expr.strip().strip("'")
            
            # Get actual values
            tuple_value = str(tuple_data[field]).strip()
            
            # Compare based on data type
            if self.schema[field] in ('varchar', 'char'):
                tuple_value = tuple_value.strip()
                value = value.strip()
                return self._compare_values(tuple_value, value, op)
            elif self.schema[field] == 'integer':
                return self._compare_values(int(tuple_value), int(value), op)
            elif self.schema[field] == 'date':
                # Add date comparison if needed
                return tuple_value == value
            
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
            # Replace aggregate function references with actual values
            condition = self.having
            for f in self.f_vect:
                if f in condition:
                    if '_avg_' in f:
                        if entry[f + '_count'] > 0:
                            value = entry[f + '_sum'] / entry[f + '_count']
                        else:
                            value = 0
                    else:
                        value = entry[f]
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
                row[attr] = entry[attr]
            
            # Add aggregate values
            for attr in self.select_attrs:
                if attr in self.grouping_attrs:
                    continue
                if '_avg_' in attr:
                    if entry[attr + '_count'] > 0:
                        row[attr] = round(entry[attr + '_sum'] / entry[attr + '_count'], 2)
                    else:
                        row[attr] = 0
                else:
                    row[attr] = entry[attr]
            results.append(row)
        
        # Sort results by grouping attributes
        return sorted(results, key=lambda x: [str(x[attr]) for attr in self.grouping_attrs])