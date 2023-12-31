package net.blergh;

public record RelName(
        String schemaName,
        String tableName
)
{
    public RelName
    {
        // it may be preferable to let users hang themselves...

        if( schemaName.contains(".") || tableName.contains(".") )
        {
            throw new IllegalArgumentException("name parts may not contain dots");
        }

        if( schemaName.contains("\"") || tableName.contains("\"") )
        {
            throw new IllegalArgumentException("name parts may not contain double quotes");
        }
    }

    public static RelName fromString(String fullName)
    {
        String[] parts = fullName.split("\\.");
        if( parts.length == 1 || parts.length > 2 )
        {
            //TODO: support 3-part names
            //TODO: support 1-part names???
            //TODO (bigger): support more exotic names, like schemas with dots (...or maybe don't, might be better off banning that stuff)
            throw new IllegalArgumentException("only a single dot separator is supported");
        }
        return new RelName(parts[0], parts[1]);
    }

    public String toFullName()
    {
        return this.schemaName + "." + this.tableName;
    }
}
