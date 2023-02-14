file1 = open('/Users/zi/Documents/gitlab/dataform_hubspot_source/includes/functions.js', 'r')
Lines = file1.readlines()
  
count = 0
# Strips the newline character
lines = []
for line in Lines:
    count += 1
    if 'function' in line:
        lines.append(line.replace('\n', ''))
for line in lines:
    print(line)
