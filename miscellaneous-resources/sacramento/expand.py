import csv

# open and read the original csv into a list of lists
with open('original.csv','r',newline='') as csvfile:
    reader = csv.reader(csvfile)

    # read in each line as a list  
    lines = [list(i) for i in reader]


out = []

# expand the list of lists
for line in lines[1:]:
    
    # get a list of related xrefs
    related = [i.strip() for i in line[2].split(',')]

    # for each related element, we want to output the
    # original row with the associated xrefs replaced
    for r in related:
        line[2] = r
        
        # save a copy of the list, because we may change it
        out.append(line[:])

# create the csv output file
with open('expanded.csv','w',newline='') as csvfile:
	writer = csv.writer(csvfile, delimiter=',', quotechar='"')
	
	# write the header row
	writer.writerow(lines[0])

	# write each row
	for row in out:
		_ = writer.writerow(row)
