require 'yaml'
require_relative 'shuttlefy'

data = YAML.load_file("output4.yaml")

# Define FQN variables
data["properties"].each do |prop|
	pcol 		= prop["column"]
	pname 		= prop["name"]
	pnamespace 	= prop["namespace"]

	pvar 		= get_var_property_fqn(pcol)
	pval 		= get_val_fqn(pnamespace, pname)
	# puts define_fqn( pvar, pval )
end

# Create property types
data["properties"].each do |prop|
	pcol 		= prop["column"]
	ptitle 		= prop["title"]
	pdescription = prop["description"]
	ptype 		= prop["type"]
	ppii 		= prop["pii"]
	panalyzer 	= prop["analyzer"]

	pvar 		= get_var_created(pcol)
	pfqn 		= get_var_property_fqn(pcol)
	# puts create_prop( pvar, pfqn, ptitle, pdescription, ptype, ppii, panalyzer)
					  
end

# Define entity type variables
data["entities"].each do |entity|
	etalias 		= entity["type"]["alias"]
	esname 			= entity["set"]["name"]
	etname 			= entity["type"]["name"]
	etnamespace		= entity["type"]["namespace"]
	etkey_count 	= entity["type"]["keys"]

	esname_var 		= get_var_entity_set_name( etalias )
	etvar 			= get_var_entity_set_type( etalias )
	etfqn 			= get_val_fqn( etnamespace, etname)
	etkey 			= get_var_entity_set_keys( etalias, etkey_count )
	etalias			= get_var_entity_set_alias( etalias )

	# puts define_entity_types(esname_var, esname, etvar, etfqn, etkey, etalias, etalias)
end

# create entity types
data["entities"].each do |entity|
	etalias 		= entity["type"]["alias"]
	etkey_count 	= entity["type"]["keys"]
	title			= entity["type"]["title"]
	description		= entity["type"]["description"]

	created_etprop 	= get_var_created( etalias )
	etvar 			= get_var_entity_set_type( etalias )

	puts create_entity_types(created_etprop, etvar, etkey_count, title, description)
end

# create association types
data["associations"].each do |association|
	atalias 		= association["type"]["alias"]
	atkey_count 	= association["type"]["keys"]
	title			= association["type"]["title"]
	description		= association["type"]["description"]
	source			= association["type"]["source"]
	destination		= association["type"]["destination"]
	direction		= association["type"]["bi"]

	created_etprop 	= get_var_created( atalias )
	etvar 			= get_var_entity_set_type( atalias )

	puts create_entity_types(created_etprop, etvar, etkey_count, title, description)
end

