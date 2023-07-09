package net.blergh.task;

import net.blergh.RelName;

public interface Task //TODO: should this be sealed?
{
    RelName getRelName();

    boolean isRunning();
}
