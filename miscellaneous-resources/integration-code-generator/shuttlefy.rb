# Code generation
def define_fqn(var, val)
	"public static FullQualifiedName #{var} = new FullQualifiedName(\"#{val}\");"
end

def create_prop(pvar, fvar, title, description, type, pii, analyzer)
"UUID #{pvar} = edm
        .createPropertyType( new PropertyType(
                Optional.absent(),
                #{fvar},
                \"#{title}\",
                Optional.of( \"#{description}\" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.#{type.capitalize},
                Optional.of( #{pii} ),
                Optional.of( Analyzer.#{analyzer.upcase} ) ) );
if ( #{pvar} == null ) {
    #{pvar} = edm
            .getPropertyTypeId( #{fvar}.getNamespace(), #{fvar}.getName() );
}"
end

def create_entity_type(tvar, fvar, title, description)
"
LinkedHashSet<UUID> #{tvar}Key = new LinkedHashSet<UUID>();
#{tvar}Key.add( REPLACEME );

LinkedHashSet<UUID> #{tvar}Properties = new LinkedHashSet<UUID>();
#{tvar}Properties.add( REPLACEME );

UUID #{tvar}Type = edm.createEntityType( new EntityType(
        #{fvar},
        \"#{title}\",
        \"#{description}\",
        ImmutableSet.of(),
        #{tvar}Key,
        #{tvar}Properties,
        Optional.absent(),
        Optional.of( SecurableObjectType.EntityType ) ) );
if ( #{tvar}Type == null ) {
    #{tvar}Type = edm.getEntityTypeId(
            #{fvar}.getNamespace(), #{fvar}.getName() );
}
"
end

def define_entity_types(esname_var, esname, estype_var, estype, estype_key_var, estype_alias_var, estype_alias)
"
public static String #{esname_var} = \"#{esname}\";
public static FullQualifiedName #{estype_var} = new FullQualifiedName( \"#{estype}\" );
#{estype_key_var}
public static String #{estype_alias_var} = \"#{estype_alias}\";
"
end

def create_entity_types(etkeypropvar, etvar, etkeycount, title, description)
output = ""
output << "LinkedHashSet<UUID> #{etkeypropvar}Key = new LinkedHashSet<UUID>();\n"

etkeycount.times do |n|
    output << "#{etkeypropvar}Key.add( REPLACEME );\n"
end

output << "LinkedHashSet<UUID> #{etkeypropvar}Properties = new LinkedHashSet<UUID>();\n"
etkeycount.times do |n|
    output << "#{etkeypropvar}Properties.add( REPLACEME );\n"
end


output << "UUID #{etkeypropvar}Type = edm.createEntityType( new EntityType(
        #{etvar},
        \"#{title}\",
        \"#{description}\",
        ImmutableSet.of(),
        #{etkeypropvar}Key,
        #{etkeypropvar}Properties,
        Optional.absent(),
        Optional.of( SecurableObjectType.EntityType ) ) );
if ( #{etkeypropvar}Type == null ) {
    #{etkeypropvar}Type = edm.getEntityTypeId(
            #{etvar}.getNamespace(), #{etvar}.getName() );
}"

end

def create_association_types()




# namespace: publicsafety
# name: arrestid
# returns: publicsafety.arrestid
def get_val_fqn(pnamespace, pname)
	"#{pnamespace.downcase}.#{pname}"
end

# column: "Arrest ID"
# returns: arrestid
def get_var_created(column)
	column.gsub(/\s/,'').downcase
end

# base: bookings
# returns: BOOKINGS_ENTITY_SET_NAME
def get_var_entity_set_name(base)
    "#{base.upcase}_ENTITY_SET_NAME"
end

# base: bookings
# returns: BOOKINGS_ENTITY_SET_TYPE
def get_var_entity_set_type(base)
    "#{base.upcase}_ENTITY_SET_TYPE"
end

# base: bookings
# returns: BOOKINGS_ALIAS
def get_var_entity_set_alias(base)
    "#{base.upcase}_ALIAS"
end

# base: bookings
# returns: public static FullQualifiedName BOOKINGS_ENTITY_SET_TYPE_KEY_# = REPLACEME_WITH_FQN;
def get_var_entity_set_keys(base, count)
    keys = ""
    count.times do |n|
        keys << "public static FullQualifiedName #{base.upcase}_ENTITY_SET_TYPE_KEY_#{n+1} = REPLACEME_WITH_FQN;\n"
    end
    return keys
end

# column: Arrest ID
# returns: ARREST_ID_FQN
def get_var_property_fqn(column)
    column.gsub(/\s/,'_').upcase + "_FQN"
end


