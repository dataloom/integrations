import csv, sys

IN_CSV = 'cad_pre.csv'
OUT_CSV = 'cad_post.csv'

############################
##### READ IN THE FILE #####
############################

# open and read the original csv into a list of lists
with open(IN_CSV,'r',newline='') as csvfile:
    reader = csv.reader(csvfile)

    # read in each line as a list  
    lines = [list(i) for i in reader]


############################
### EXPAND RELATED XREFS ###
############################
    
expanded = []

# expand the list of lists
for line in lines[1:]:
    
    # get a list of related xrefs
    related = [i.strip() for i in line[2].split(',')]

    # for each related element, we want to output the
    # original row with the associated xrefs replaced
    for r in related:
        line[2] = r
        
        # save a copy of the list, because we may change it
        expanded.append(line[:])

############################
## BRACKETS -> PLACE NAME ##
############################

commented = [] # this step's output

for line in expanded:
        line = line[:] # make a copy just in case
		
        if '[' in line[3]:
				# split the line into address and comment
                addr, comment = line[3].split('[', 1)
				
				# the address only keeps the true address portion
                line[3] = addr.strip()
				
				# separate comments with a comma only if previous comment was not blank
                if line[5] == '':
                        line[5] = comment
                else:
                        line[5] = '%s, %s' % (comment, line[5])
		
		# add line to output
        commented.append(line)


############################
#### STRIP EXTRA SPACES ####
############################

stripped = [] # this step's output

for line in commented:
        line = line[:] # copy line just in case
		
        stripped.append([]) # new list for this line
		
		# keep adding stripped elements from the line to the latest list in 'stripped'
        for i in line:
                stripped[-1].append(i.strip())

############################
####### WRITE TO CSV #######
############################

# create the csv output file
with open(OUT_CSV,'w',newline='') as csvfile:
	writer = csv.writer(csvfile, delimiter=',', quotechar='"')
	
	# write the header row
	writer.writerow(lines[0])

	# write each row
	for row in stripped:
		_ = writer.writerow(row)
