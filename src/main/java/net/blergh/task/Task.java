package net.blergh.task;

import net.blergh.RelName;
import net.blergh.TaskStatus;

import java.util.Optional;

public interface Task //TODO: should this be sealed?
{
    RelName getRelName();

    boolean isRunning();

    Optional<TaskStatus> getKnownTaskStatus();
}
