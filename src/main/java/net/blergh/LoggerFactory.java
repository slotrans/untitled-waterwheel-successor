package net.blergh;

import org.slf4j.Logger;

public class LoggerFactory
{
    public static Logger make()
    {
        Throwable t = new Throwable();
        StackTraceElement directCaller = t.getStackTrace()[1];
        return org.slf4j.LoggerFactory.getLogger( directCaller.getClassName() );
    }

    public static Logger make( String name )
    {
        return org.slf4j.LoggerFactory.getLogger( name );
    }

    public static Logger make( Class clazz )
    {
        return org.slf4j.LoggerFactory.getLogger( clazz );
    }
}
