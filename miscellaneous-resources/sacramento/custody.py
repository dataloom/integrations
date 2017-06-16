import csv, sys

IN_CSV = 'custody_pre.csv'
OUT_CSV = 'custody_post.csv'

############################
##### READ IN THE FILE #####
############################

# open and read the original csv into a list of lists
with open(IN_CSV,'r',newline='') as csvfile:
    reader = csv.reader(csvfile)

    # read in each line as a list  
    lines = [list(i) for i in reader]


############################
# FIX DESCRIPTION SPELLING #
############################

# replace 'Descrition' with 'Description' in first line
for idx in range(len(lines[0])):
    lines[0][idx] = lines[0][idx].replace('Descrition', 'Description')
    

############################
## REMOVE MID-FILE HEADER ##
############################

# take the first row, plus any future rows that do not contain 'CustodyID'
hl_lines = [lines[0]]
for line in lines[1:]:
	if not 'CustodyID' in str(line):
		hl_lines.append(line)

###########################
## REMOVE OUTER SPACES ####
###########################

stripped_lines = [] # this step's output

for line in hl_lines:
    stripped_lines.append([]) # new list for this line 
	
	# keep adding stripped elements to the latest line in stripped_lines
    for x in line: 
        stripped_lines[-1].append(x.strip())


############################
### REMOVE DOUBLE SPACES ###
############################

clean_lines = [] # this step's output

for line in stripped_lines:
    clean_lines.append([]) # new list for this line

	# for each element, remove multiple spaces then append to latest line in clean_lines
    for x in line:
		# while there's a double space, truncate it
        while '  ' in x:
            x = x.replace('  ',' ')
        clean_lines[-1].append(x)


############################
####### WRITE TO CSV #######
############################

# create the csv output file
with open(OUT_CSV,'w',newline='') as csvfile:
	writer = csv.writer(csvfile, delimiter=',', quotechar='"')
	
	# write the header row
	writer.writerow(lines[0])

	# write each row
	for row in clean_lines[1:]:
		_ = writer.writerow(row)
