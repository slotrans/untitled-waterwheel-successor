package net.blergh.task;

import net.blergh.LoggerFactory;
import net.blergh.RelName;
import net.blergh.TaskStatus;
import org.slf4j.Logger;

import java.util.Optional;

/* TODO
    In theory there could be a number of kinds of source tasks:
    - "simple" sources that are assumed to exist and basically go straight to COMPLETED
    - "existence checking" sources that verify a relation exists (and therefore can fail if it doesn't)
    - "freshness checking" sources that verify their data is sufficiently up-to-date
    - etc
 */
public class SourceTask implements Task
{
    private static final Logger log = LoggerFactory.make();

    private final RelName relName;


    public SourceTask(RelName relName)
    {
        this.relName = relName;
    }

    @Override
    public RelName getRelName()
    {
        return relName;
    }

    @Override
    public Optional<TaskStatus> getKnownTaskStatus()
    {
        return Optional.of(TaskStatus.COMPLETE);
    }

    @Override
    public void upstreamFailureObserved()
    {
        throw new UnsupportedOperationException("a SourceTask having a failed dependency makes no sense!");
    }

    @Override
    public boolean start()
    {
        throw new UnsupportedOperationException("a SourceTask should never be started!");
    }

    @Override
    public int getThreadWeight()
    {
        return 0;
    }

    @Override
    public String toString()
    {
        return String.format("SourceTask(%s)", relName.toFullName());
    }
}
