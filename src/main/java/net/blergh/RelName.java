package net.blergh;

public record RelName(
        String schemaName,
        String tableName
)
{
    // see pack_node_name(schema_name, table_name)
    public static RelName fromString(String fullName)
    {
        String[] parts = fullName.split("\\.");
        if( parts.length > 2 )
        {
            //TODO: support 3-part names
            //TODO (bigger): support more exotic names, like schemas with dots (...or maybe don't, might be better off banning that stuff)
            throw new IllegalArgumentException("only a single dot separator is supported");
        }
        return new RelName(parts[0], parts[1]);
    }

    // see unpack_node_name(node_name)
    public String toFullName()
    {
        return this.schemaName + "." + this.tableName;
    }
}
