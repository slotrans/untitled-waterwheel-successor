package net.blergh.task;

import net.blergh.LoggerFactory;
import net.blergh.RelName;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;

import java.util.concurrent.FutureTask;

public class SqlTask implements Task
{
    private static final Logger log = LoggerFactory.make();

    private final RelName relName;
    private final String createScript;
    private final String buildScript;
    private final BuildMode buildMode;

    private final Jdbi jdbi;

    private FutureTask<String> futureTask; //TODO: should return a record describing its execution, not just the output but also the runtime, etc
    private Thread threadForFuture;


    public SqlTask(RelName relName, String createScript, String buildScript, BuildMode buildMode, Jdbi jdbi)
    {
        this.relName = relName;
        this.createScript = createScript;
        this.buildScript = buildScript;
        this.buildMode = buildMode;

        this.jdbi = jdbi;
    }


    /**
     * @return true if a thread was started, false otherwise
     */
    public boolean start()
    {
        if( futureTask != null )
        {
            return false;
        }

        futureTask = new FutureTask<>(this::runTransformation);
        threadForFuture = new Thread(futureTask);
        threadForFuture.start();

        return true;
    }

    private String runTransformation()
    {
        String output = "";

        if( buildMode == BuildMode.FULL )
        {
            output += runCreate();
        }

        output += runBuild();

        return output;
    }

    private String runCreate()
    {
        /* something like...
        jdbi.withHandle(handle -> {
            StringBuilder output = new StringBuilder();

            String dropStatement = String.format("drop table if exists \"%s\".\"%s\"", relName.schemaName(), relName.tableName());
            log.info(dropStatement);
            output.append(dropStatement);

            log.info(createScript);
            output.append(createScript);
            handle.createScript(createScript).execute();

            String grantStatement = String.format("grant select on \"%s\".\"%s\" to common_read_role", relName.schemaName(), relName.tableName());
            log.info(grantStatement);
            output.append(grantStatement);

            return output.toString();
        });
        */
        return "(table creation output for " + relName + ")";
    }

    private String runBuild()
    {
        return "(build script output for " + relName + ")";
    }

    @Override
    public boolean isRunning()
    {
        if( futureTask == null )
        {
            return false;
        }

        if( futureTask.isDone() || futureTask.isCancelled() )
        {
            return false;
        }

        return true;
    }

    //TODO: public Optional<String> getResult() or something

    @Override
    public RelName getRelName()
    {
        return relName;
    }
}
