require 'csv'

@header = ""

CSV.open(ARGV[0], 'r') do |csv|
	@header = csv.first
end

output = File.open(ARGV[1],"w")

output << "---\n"
output << "properties:"
@header.each do |h|
output <<  "
- column: #{h}
  namespace:
  name:
  title:
  description:
  pii:
  type:
  analyzer:"
end

output << "\n\nassociation:"
output << "
- title: 
  description: 
  name: 
  type:
  - namespace: 
    name: 
    keys: 
    alias: 
    source: 
    destination: 
    bi: 
"

output << "\n\nentities:"
output << "
- title: 
  description: 
  name: 
  type:
  - title:
  	description:
  	namespace: 
    name: 
    keys: 
    alias: 
"