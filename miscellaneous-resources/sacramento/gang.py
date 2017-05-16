import csv, sys

IN_CSV = 'gang_pre.csv'
OUT_CSV = 'gang_post.csv'

############################
##### READ IN THE FILE #####
############################

# open and read the original csv into a list of lists
with open(IN_CSV,'r',newline='') as csvfile:
    reader = csv.reader(csvfile)

    # read in each line as a list  
    lines = [list(i) for i in reader]


############################
# REPLACE NULLS WITH BLANK #
############################
    
blank = [] # this step's output

for line in lines[1:]:

	# copy over the first three elements (WITHOUT PROCESSING) into new list
    blank.append(line[:3])

	# for all other elements, we copy either them or, if NULL, ''
    for i in line[3:]:
        if i.strip() == 'NULL':
            i = ''
        blank[-1].append(i.strip()) # strip extra spaces for good measure

############################
####### WRITE TO CSV #######
############################

# create the csv output file
with open(OUT_CSV,'w',newline='') as csvfile:
	writer = csv.writer(csvfile, delimiter=',', quotechar='"')
	
	# write the header row
	writer.writerow(lines[0])

	# write each row
	for row in blank:
		_ = writer.writerow(row)
