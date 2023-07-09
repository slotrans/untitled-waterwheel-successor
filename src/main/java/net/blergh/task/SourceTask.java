package net.blergh.task;

import net.blergh.LoggerFactory;
import net.blergh.RelName;
import org.slf4j.Logger;

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

    private RelName relName;


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
    public boolean isRunning()
    {
        return false;
    }
}
