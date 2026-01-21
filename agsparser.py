# Updated parsing logic to exclude lines starting with <UNITS> or <CONT>

# Assuming _split_quoted_csv is a function that splits the CSV strings appropriately.
for line in lines:
    # Split the line using the existing method
    split_line = _split_quoted_csv(line)
    
    # Check if the first part is <UNITS> or <CONT>
    if split_line[0] != '<UNITS>' and split_line[0] != '<CONT>':
        # Process the line further
        pass  # Replace with actual processing logic
