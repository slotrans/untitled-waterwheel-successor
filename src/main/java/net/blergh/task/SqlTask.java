package net.blergh.task;

import net.blergh.LoggerFactory;
import net.blergh.RelName;
import net.blergh.TaskStatus;
import org.apache.commons.math3.util.Precision;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.JdbiException;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.FutureTask;

public class SqlTask implements Task
{
    private static final Logger log = LoggerFactory.make();

    private final RelName relName;
    private final String createScript;
    private final String buildScript;
    private final BuildMode buildMode;

    private final Jdbi jdbi;
    //TODO: probably needs a timeout, though you may prefer to lean on the DB for that

    //INVARIANT: knownTaskStatus must be kept in-sync with respect to the FutureTask and Thread
    private volatile Optional<TaskStatus> knownTaskStatus;
    private FutureTask<String> futureTask; //TODO: should return a record describing its execution, not just the output but also the runtime, etc
    private Thread threadForFuture;


    public SqlTask(RelName relName, String createScript, String buildScript, BuildMode buildMode, Jdbi jdbi)
    {
        this.relName = relName;
        this.createScript = createScript;
        this.buildScript = buildScript;
        this.buildMode = buildMode;

        this.jdbi = jdbi;

        this.knownTaskStatus = Optional.empty();
    }


    /**
     * @return true if a thread was started, false otherwise
     */
    public boolean start()
    {
        if( knownTaskStatus.isPresent() )
        {
            return false;
        }

        futureTask = new FutureTask<>(this::runTransformation);
        threadForFuture = new Thread(futureTask);
        threadForFuture.start();

        knownTaskStatus = Optional.of(TaskStatus.RUNNING);

        return true;
    }

    private String runTransformation()
    {
        try
        {
            if( buildMode == BuildMode.FULL )
            {
                log.info("running create for {}", relName);
                runCreate();
            }

            runBuild();

            log.info("{}: runTransformation() completed", relName);
            safelySetKnownTaskStatus(TaskStatus.COMPLETE);
        }
        catch( JdbiException e )
        {
            log.error("{}: JdbiException caught", relName, e);
            safelySetKnownTaskStatus(TaskStatus.FAILED);
        }

        return knownTaskStatus.toString(); //need to return something to satisfy Callable, probably need to re-think this
    }

    private void runCreate()
    {
        jdbi.useHandle( handle -> {
            String dropStatement = String.format("drop table if exists \"%s\".\"%s\"", relName.schemaName(), relName.tableName());
            log.info(dropStatement);
            handle.execute(dropStatement);

            //TODO: run this in a transaction
            log.info(createScript);
            handle.createScript(createScript).execute(); //unfortunate naming coincidence here...

            //String grantStatement = String.format("grant select on \"%s\".\"%s\" to common_read_role", relName.schemaName(), relName.tableName());
            //log.info(grantStatement);
        });
    }

    private void runBuild()
    {
        //TODO: create a configurable _waterwheel_global temp table here, or some general pre-script

        //TODO: also a general post-script

        jdbi.useHandle( handle -> {
            log.info(buildScript);
            final long buildStartNanos = System.nanoTime();
            handle.createScript(buildScript).execute();
            final long buildStopNanos = System.nanoTime();
            final double ONE_BILLION = 1_000_000_000;
            final double durationSeconds = Precision.round( (buildStopNanos - buildStartNanos) / ONE_BILLION, 3);
            log.info("{}: build complete in {} seconds", relName, durationSeconds);

            String analyzeStatement = String.format("analyze \"%s\".\"%s\"", relName.schemaName(), relName.tableName());
            log.info(analyzeStatement);
            handle.execute(analyzeStatement);
        });
    }

    private synchronized void safelySetKnownTaskStatus(TaskStatus taskStatus)
    {
        knownTaskStatus = Optional.of(taskStatus);
    }


    @Override
    public RelName getRelName()
    {
        return relName;
    }

    @Override
    public Optional<TaskStatus> getKnownTaskStatus()
    {
        return knownTaskStatus;
    }

    @Override
    public void upstreamFailureObserved()
    {
        if( knownTaskStatus.isEmpty() )
        {
            safelySetKnownTaskStatus(TaskStatus.UPSTREAM_FAILED);
        }
    }

    @Override
    public int getThreadWeight()
    {
        //TODO: make this configurable per-task
        return 1;
    }

    @Override
    public String toString()
    {
        return String.format("SqlTask(%s)", relName.toFullName());
    }
}
